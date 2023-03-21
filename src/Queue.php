<?php

namespace icy8\Queue;

use icy8\Queue\connector\Connector;

/**
 * 一些用户可安全操作的别名方法
 * Class Queue
 * @package icy8\queue
 */
class Queue
{
    /* @var Connector $connector */
    protected $connector;

    public function __construct($driver, $config = [])
    {
        $class = 'icy8\\Queue\\connector\\' . ucwords($driver);
        if (!class_exists($class)) {
            throw new \Exception("driver class not found: {$class}");
        }
        $this->connector = new $class($config);
    }

    /**
     * @param $job
     * @param string $data
     * @param int $maxTries
     */
    public function push($job, $data = '', $maxTries = 0)
    {
        $this->connector->push($job, $data, $maxTries);
    }

    /**
     * @param $deley
     * @param $job
     * @param string $data
     * @param int $maxTries
     */
    public function delay($deley, $job, $data = '', $maxTries = 0)
    {
        // @todo $deley参数可能会带来歧义，但对$data可选操作会更友好
        $this->connector->pushDelay($job, $data, $deley, $maxTries);
    }
}