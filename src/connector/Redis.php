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
        if (is_array($config) && !empty($config)) {
            $this->setConfig($config)->init();
        }
    }

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

    public function push($job, $data = '', $maxTries = 0, $queue = null)
    {
        $payload_json = $this->createPayload($job, $data, $maxTries);
        $this->redis->rPush($this->queueName($queue), $payload_json);
    }

    public function pushDelayRaw($payload, $delay, $queue = null)
    {
        $this->redis->zAdd($this->queueName($queue) . ':delay', $this->availableAt($delay), $payload);
    }

    public function pop()
    {
        $this->mergeJobs();
        $payload  = $this->redis->lPop($this->queueName());
        $reserved = false;
        if (!$payload) {
            return;
        }
        // 添加一个预备任务
        $reserved = json_decode($payload, true);
        $reserved['retried_times']++;
        $reserved = json_encode($reserved);
        $this->redis->zAdd($this->queueName() . ':reserved', $this->availableAt($this->retryInterval), $reserved);
        return new RedisDispatcher($this, $payload, $this->defaultName, $reserved);
    }

    /**
     * 合并延迟的任务
     */
    public function mergeJobs()
    {
        $this->mergeDelayJobs($this->queueName() . ':delay');
        if (!is_null($this->retryInterval)) {
            $this->mergeDelayJobs($this->queueName() . ':reserved');
        }
    }

    protected function mergeDelayJobs($queue)
    {
        $this->redis->watch($queue);
        // 提取分数小于当前时间的集合
        $jobs = $this->redis->zRangeByScore($queue, '-inf', time());
        if (!empty($jobs)) {
            $this->transaction(function () use ($queue, $jobs) {
                $this->redis->zRemRangeByRank($queue, 0, count($jobs) - 1);
                $chunk = array_chunk($jobs, 100);
                foreach ($chunk as $list) {
                    $this->redis->rPush($this->queueName(), ...$list);
                }
            });
        }
        $this->redis->unwatch();
    }

    public function deleteReserved($queue, RedisDispatcher $dispatcher)
    {
        $this->redis->zRem($this->queueName($queue . ':reserved'), $dispatcher->getReserved());
    }

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

    protected function queueName($name = '')
    {
        return "icy8:queue:" . ($name ?: $this->defaultName);
    }
}
