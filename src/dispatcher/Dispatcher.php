<?php

namespace icy8\Queue\dispatcher;

use icy8\Queue\connector\Connector;

abstract class Dispatcher
{
    protected $id;
    protected $raw;
    protected $queue;
    /* @var Connector $connector */
    protected $connector;
    protected $instance;
    protected $failed  = false;
    protected $deleted = false;

    public function __construct($connector, $raw, $queue)
    {
        $this->queue     = $queue;
        $this->raw       = $raw;
        $this->connector = $connector;
        $this->id        = uniqid('queue:');
    }

    public function payload()
    {
        return json_decode($this->raw, true);
    }

    public function dispatch()
    {
        $payload = $this->payload();
        $job     = $payload['job'];
        $data    = $payload['data'];
        [$this->instance, $method] = $this->resolveJob($job);
        if ($this->instance) {
            $this->instance->{$method}($this, $data);
        }
    }

    public function delete()
    {
        $this->deleted = true;
    }

    public function fail(\Throwable $e)
    {
        $this->failed = true;
        if (method_exists($this->instance, 'onFail')) {
            $payload = $this->payload();
            $this->instance->onFail($this, $payload['data'], $e);
        }
    }

    public function isFail()
    {
        return $this->failed;
    }

    public function isDelete()
    {
        return $this->deleted;
    }

    public function expiredAt()
    {
        return $this->payload()['expired_at'] ?? null;
    }

    public function retriedTimes()
    {
        return $this->payload()['retried_times'] ?? null;
    }

    public function maxRetriedTimes()
    {
        return $this->payload()['max_retried_times'] ?? null;
    }

    /**
     * 重新发布当前任务
     * @param mixed $delay
     */
    public function republish($delay = null)
    {
        $payload = $this->payload();
        // 重置重试次数
        $payload['retried_times'] = 0;
        //
        $method = 'pushRaw';
        if ($delay) {
            $method = 'pushDelayRaw';
        }
        $this->connector->{$method}(json_encode($payload));
    }

    protected function resolveJob($job)
    {
        $class  = $job[0] ?? null;
        $method = $job[1] ?? null;
        if (!class_exists($class)) {
            throw new \Exception('`job` class not found');
        }
        $method   = $method ?: 'handle';
        $instance = new $class;
        if (!method_exists($instance, $method)) {
            throw new \Exception("method not found: {$class}@{$method}");
        }
        return [$instance, $method];
    }

    /**
     * @return string
     */
    public function getId()
    {
        return $this->id;
    }

}
