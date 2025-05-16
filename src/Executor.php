<?php

namespace icy8\Queue;

use icy8\Queue\connector\Connector;
use icy8\Queue\dispatcher\Dispatcher;
use icy8\Queue\exception\JobExpiredException;
use icy8\Queue\exception\MaxTriedExceedException;

/**
 * 队列消费
 * Class Executor
 */
class Executor
{

    /* @var Connector $connector */
    protected $connector;

    public function __construct($driver = 'redis', $config = [])
    {
        $class = 'icy8\\Queue\\connector\\' . ucwords($driver);
        if (!class_exists($class)) {
            throw new \Exception("driver class not found: {$class}");
        }
        $this->connector = new $class($config);
    }

    public function runNext()
    {
        $dispatcher = $this->next();
        if (!$dispatcher) return;
        $id     = $dispatcher->getId();
        $output = new Output();
        try {
            // fatal error
            register_shutdown_function(function () use ($dispatcher, $output) {
                $error = error_get_last();
                if ($error && in_array($error['type'], [E_ERROR, E_PARSE, E_CORE_ERROR, E_COMPILE_ERROR])) {
                    $dispatcher->fail(new \Exception($error['message']));
                    $output->error("fatal error " . $dispatcher->getId());
                } else if (!$dispatcher->isFinished()) {
                    $dispatcher->finish();
                }
            });
            $this->whenJobInvalidate($dispatcher);
            $output->info("start {$id}");
            $dispatcher->dispatch();
            $output->info("success {$id}");
        } catch (\Throwable $e) {
            if (!$dispatcher->isFail()) {
                $dispatcher->fail($e);
                $output->error("fail {$id}");
            }
        }
    }

    /**
     * @return Dispatcher
     */
    protected function next()
    {
        return $this->connector->pop();
    }

    protected function whenJobInvalidate(Dispatcher $dispatcher)
    {
        $expired_at        = $dispatcher->expiredAt();
        $retry_times       = $dispatcher->retriedTimes();
        $max_retried_times = $dispatcher->maxRetriedTimes();
        $e                 = null;
        if ($expired_at && $expired_at < time()) {
            $e = new JobExpiredException('Task overdue');
        } else if (($max_retried_times === 0 && $retry_times > 0) || ($max_retried_times > 0 && $retry_times >= $max_retried_times)) {
            $e = new MaxTriedExceedException('The number of attempts exceeds ' . $max_retried_times . ' times');
        }
        if ($e instanceof \Exception) {
            $this->failJob($dispatcher, $e);
            throw $e;
        }
    }

    protected function failJob(Dispatcher $dispatcher, $e)
    {
        if ($dispatcher->isDelete()) {
            return;
        }
        $dispatcher->delete();
        $dispatcher->fail($e);
    }

    public function getConnector()
    {
        return $this->connector;
    }
}
