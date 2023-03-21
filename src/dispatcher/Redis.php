<?php

namespace icy8\Queue\dispatcher;
class Redis extends Dispatcher
{
    protected $reserved;

    public function __construct($connector, $raw, $queue, $reserved)
    {
        parent::__construct($connector, $raw, $queue);
        $this->reserved = $reserved;
    }

    /**
     * 删除一个任务
     */
    public function delete()
    {
        parent::delete();
        // 同时将预备任务删掉，避免异常导致无限重试
        $this->connector->deleteReserved($this->queue, $this);
    }

    /**
     * @return mixed
     */
    public function getReserved()
    {
        return $this->reserved;
    }
}