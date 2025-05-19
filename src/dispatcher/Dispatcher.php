<?php

namespace icy8\Queue\dispatcher;

use icy8\Queue\connector\Connector;

abstract class Dispatcher
{
    protected $id;
    protected $raw;
    /* @var Connector $connector */
    protected $connector;
    protected $instance;
    protected $failed      = false;
    protected $finished    = false;
    protected $deleted     = false;
    protected $republished = false;
    protected $_data       = [];//

    public function __construct($connector, $raw)
    {
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
            if (method_exists($this->instance, 'onStart')) {
                try {
                    $this->instance->onStart($this, $data);
                } catch (\Throwable $e) {
                }
            }
            $this->instance->{$method}($this, $data);
            $this->delete();
            $this->finish();
        }
    }

    public function delete()
    {
        $this->deleted = true;
    }

    public function finish()
    {
        $this->finished = true;
        $payload        = $this->payload();
        if (method_exists($this->instance, 'onFinish')) {
            try {
                $this->instance->onFinish($this, $payload);
            } catch (\Throwable $e) {
            }
        }
    }

    public function fail(\Throwable $e)
    {
        $this->failed = true;
        $payload      = $this->payload();
        // 重试
        if ($this->retriedTimes() < $this->maxRetriedTimes()) {
            $this->setRetriedTimes($this->retriedTimes() + 1);
            $payload = $this->payload();
            $this->republish($payload['delay'] ?? 0, true);
            //
            if ($this->retriedTimes() >= $this->maxRetriedTimes()) {
                $this->republished = false;
            }
        }
        if (method_exists($this->instance, 'onFail')) {
            try {
                $this->instance->onFail($this, $payload['data'], $e);
            } catch (\Throwable $e) {
            }
        }
        $this->finish();
    }

    public function isFail()
    {
        return $this->failed;
    }

    public function isDelete()
    {
        return $this->deleted;
    }

    public function isRepublish()
    {
        return $this->republished;
    }

    public function isFinished()
    {
        return $this->finished;
    }

    public function expiredAt()
    {
        return $this->payload()['expired_at'] ?? null;
    }

    public function retriedTimes()
    {
        return $this->payload()['retried_times'] ?? 0;
    }

    protected function setRetriedTimes($times)
    {
        $payload                  = $this->payload();
        $payload['retried_times'] = $times;
        $this->raw                = json_encode($payload);
    }

    public function maxRetriedTimes()
    {
        return $this->payload()['max_retried_times'] ?? 0;
    }

    /**
     * 重新发布当前任务
     * @param mixed $delay
     * @param bool $inheritRetriedTimes
     */
    public function republish($delay = null, $inheritRetriedTimes = false)
    {
        $this->republished = true;
        $payload           = $this->payload();
        if (!$inheritRetriedTimes) {
            $payload['retried_times'] = 0; // 重置重试的次数
        }
        $method = 'pushRaw';
        $delay && $method = 'pushDelayRaw';
        $params = array_filter([json_encode($payload), $delay]);
        call_user_func_array([$this->connector, $method], $params);
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

    public function __set($name, $value)
    {
        $this->_data[$name] = $value;
    }

    public function __get($name)
    {
        return $this->_data[$name] ?? null;
    }

}
