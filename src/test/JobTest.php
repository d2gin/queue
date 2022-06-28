<?php

namespace icy8\Queue\test;

use icy8\Queue\dispatcher\Redis;

class JobTest
{
    public function handle(Redis $job, $data)
    {
        var_dump("data:");
        var_dump($data);
//        throw new \Exception("test exception");
    }

    /**
     * 任务执行完成但失败了
     * @param $job
     * @param $data
     * @param $exception
     */
    public function onFail($job, $data, $exception)
    {
    }

    public function custom($job, $data) {
        var_dump("custom data:");
        var_dump($data);
    }
}
