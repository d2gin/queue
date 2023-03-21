<?php

namespace icy8\Queue\connector;

use icy8\Queue\exception\InvalidPayloadException;
use icy8\Queue\dispatcher\Redis as RedisDispatcher;

class Redis extends Connector
{
    protected $defaultName = 'default';
    protected $initialized = false;
    protected $config      = [
        'host'     => '127.0.0.1',
        'port'     => 6379,
        'password' => null,
    ];
    /* @var \Redis $redis */
    public $redis;

    public function __construct($config = [])
    {
        parent::__construct();
        if (is_array($config) && !empty($config)) {
            $this->setConfig($config)->init();
        }
    }

    /**
     * 初始化
     * @return $this|bool
     * @throws \Exception
     */
    public function init()
    {
        if ($this->initialized) return true;
        if (!extension_loaded('redis')) {
            throw  new \Exception('redis extension unload');
        }
        $redis = new \Redis();
        $redis->connect(
            $this->config['host'],
            $this->config['port'],
            $this->config['timeout'] ?? 0.0,
            $this->config['reserved'] ?? null,
            $this->config['retry_interval'] ?? 0,
            $this->config['read_timeout'] ?? 0.0
        );
        if ($this->config['password'] !== null) {
            $redis->auth($this->config['password']);
        }
        $this->redis       = $redis;
        $this->initialized = true;
        return $this;
    }

    /**
     * @param $payload
     * @param null $queue
     */
    public function pushRaw($payload, $queue = null)
    {
        $this->redis->rPush($this->queueName($queue), $payload);
    }

    /**
     * @param $payload
     * @param $delay
     * @param null $queue
     */
    public function pushDelayRaw($payload, $delay, $queue = null)
    {
        $this->redis->zAdd($this->queueName($queue) . ':delay', $this->availableAt($delay), $payload);
    }

    /**
     * 任务出栈
     * @return RedisDispatcher|void
     */
    public function pop()
    {
        $this->mergeJobs();
        $payload = $this->redis->lPop($this->queueName());
        if (!$payload) {
            return;
        }
        // 添加一个预备任务，就是即将重试的任务，会排在马上要执行的延时任务之后
        $reserved = json_decode($payload, true);
        $reserved['retried_times']++;
        $reserved = json_encode($reserved);
        $this->redis->zAdd($this->queueName() . ':reserved', $this->availableAt($this->retryInterval), $reserved);
        return new RedisDispatcher($this, $payload, $this->defaultName, $reserved);
    }

    /**
     * 合并任务
     */
    public function mergeJobs()
    {
        $this->mergeDelayJobs($this->queueName() . ':delay');
        if (!is_null($this->retryInterval)) {
            $this->mergeDelayJobs($this->queueName() . ':reserved');
        }
    }

    /**
     * 合并延时任务
     * @param $queue
     */
    protected function mergeDelayJobs($queue)
    {
        $this->redis->watch($queue);
        // 提取分数小于当前时间的集合
        // 即把所有到时间执行的任务推入redis队列中执行
        $jobs = $this->redis->zRangeByScore($queue, '-inf', time());
        if (!empty($jobs)) {
            $this->transaction(function () use ($queue, $jobs) {
                // 把对应的任务从集合中删除，结合上面代码可以理解为数据出栈
                $this->redis->zRemRangeByRank($queue, 0, count($jobs) - 1);
                // 一批100个任务入栈队列中
                $chunk = array_chunk($jobs, 100);
                foreach ($chunk as $list) {
                    $this->redis->rPush($this->queueName(), ...$list);
                }
            });
        }
        $this->redis->unwatch();
    }

    /**
     * 删除一个预备任务
     * @param $queue
     * @param RedisDispatcher $dispatcher
     */
    public function deleteReserved($queue, RedisDispatcher $dispatcher)
    {
        $this->redis->zRem($this->queueName($queue . ':reserved'), $dispatcher->getReserved());
    }

    /**
     * redis事务
     * @param $closure
     */
    public function transaction($closure)
    {
        $this->redis->multi();
        try {
            call_user_func_array($closure, []);
            $res = $this->redis->exec();
            if (!$res) {
                $this->redis->discard();
            }
        } catch (\Throwable $e) {
            $this->redis->discard();
        }
    }

    /**
     * 队列key
     * @param string $name
     * @return string
     */
    protected function queueName($name = '')
    {
        return "icy8:queue:" . ($name ?: $this->defaultName);
    }
}
