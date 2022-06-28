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

    public function delete()
    {
        parent::delete();
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