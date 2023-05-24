<?php

namespace icy8\Queue\connector;

use icy8\Queue\exception\InvalidPayloadException;

abstract class Connector
{
    protected $retryInterval = 1;// 默认1秒后重试
    protected $config        = [];
    public $name; // 队列名称

    public function __construct()
    {
    }

    /**
     * 出栈
     * @return mixed
     */
    abstract public function pop();

    /**
     * 按荷载数据入栈
     * @param $payload
     * @return mixed
     */
    abstract public function pushRaw($payload);

    /**
     * 按荷载数据入栈延时任务
     * @param $payload
     * @param $delay
     * @return mixed
     */
    abstract public function pushDelayRaw($payload, $delay);

    /**
     * 队列长度
     * @return mixed
     */
    abstract public function length();

    /**
     * 入栈
     * @param $job
     * @param string $data
     * @param int $maxTries
     * @throws InvalidPayloadException
     */
    public function push($job, $data = '', $maxTries = 0)
    {
        $payload_json = $this->createPayload($job, $data, $maxTries);
        $this->pushRaw($payload_json);
    }

    /**
     * 入栈延时任务
     * @param $job
     * @param $data
     * @param $delay
     * @param int $maxTries
     * @throws InvalidPayloadException
     */
    public function pushDelay($job, $data, $delay, $maxTries = 0)
    {
        $payload_json = $this->createPayload($job, $data, $maxTries);
        $this->pushDelayRaw($payload_json, $delay);
    }

    /**
     * @param int $seconds
     * @return int
     */
    public function availableAt($seconds = 0)
    {
        return time() + $seconds;
    }

    /**
     * @param $job
     * @param string $data
     * @param int $maxTries
     * @return array
     * @throws InvalidPayloadException
     */
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

    /**
     * @param $job
     * @param $data
     * @param int $maxTries
     * @return false|string
     * @throws InvalidPayloadException
     */
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
