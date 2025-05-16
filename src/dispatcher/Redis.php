<?php

namespace icy8\Queue\dispatcher;
class Redis extends Dispatcher
{
    protected $reserved;

//    public function __construct($connector, $raw, $reserved)
//    {
//        parent::__construct($connector, $raw);
//        $this->reserved = $reserved;
//    }

    /**
     * 不再使用预备任务来调度重试
     * Redis constructor.
     * @param $connector
     * @param $raw
     */
    public function __construct($connector, $raw)
    {
        parent::__construct($connector, $raw);
    }

    /**
     * 删除一个任务
     */
    public function delete()
    {
        parent::delete();
        // 同时将预备任务删掉，避免异常导致无限重试
        //$this->connector->deleteReserved($this);
    }

    /**
     * @return mixed
     */
    public function getReserved()
    {
        return $this->reserved;
    }
}