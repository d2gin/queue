<?php

namespace icy8\Queue\connector;

use icy8\Queue\exception\InvalidPayloadException;

abstract class Connector
{
    protected $retryInterval = 1;// 默认1秒后重试
    protected $config        = [];

    public function __construct()
    {
    }

    abstract public function pop();

    abstract public function pushRaw($payload);

    public function push($job, $data = '', $maxTries = 0)
    {
        $payload_json = $this->createPayload($job, $data, $maxTries);
        $this->pushRaw($payload_json);
    }


    abstract public function pushDelayRaw($payload, $delay);

    public function pushDelay($job, $data, $delay, $maxTries = 0)
    {
        $payload_json = $this->createPayload($job, $data, $maxTries);
        $this->pushDelayRaw($payload_json, $delay);
    }

    public function availableAt($seconds = 0)
    {
        return time() + $seconds;
    }

    protected function createPayloadArray($job, $data = '', $maxTries = 0)
    {
        if (is_array($job) && count($job) !== 2) {
            throw new InvalidPayloadException('`job` must be an array as [classname, method]');
        } else if (!is_array($job) && !is_string($job)) {
            throw new InvalidPayloadException('unknown `job` to point');
        }
        return [
            'id'                => uniqid('queue:'),
            'job'               => is_string($job) ? [$job] : $job,
            'data'              => $data,
            'retried_times'     => 0,
            'max_retried_times' => $maxTries,
            'expire_at'         => 0,// @todo 暂不支持任务过期配置
        ];
    }

    protected function createPayload($job, $data, $maxTries = 0)
    {
        $payload      = $this->createPayloadArray($job, $data, $maxTries);
        $payload_json = json_encode($payload);
        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new InvalidPayloadException('payload format error');
        }
        return $payload_json;
    }

    /**
     * @param array $config
     * @return Connector
     */
    public function setConfig($config)
    {
        $this->config = array_merge($this->config, $config);
        return $this;
    }
}
