<?php

namespace icy8\Queue;

use Symfony\Component\Process\PhpExecutableFinder;
use Symfony\Component\Process\PhpProcess;
use Symfony\Component\Process\Process;

/**
 * 消息监听器
 * Class Listener
 */
class Listener
{
    protected $process;
    public    $makeAutoload = true; // 自定义消费进程是否追加composer自动加载
    public    $execInterval = 1;    // 进程调度间隔 默认是1秒 允许小数
    public    $onBeforeDispatch;    // 进程调度前的事件
    public    $onAfterDispatch;     // 进程调度后的事件

    public function run($driver = 'redis')
    {
        /* @var Process $process */
        $process = $this->process;
        if (!$process) {
            $process = $this->defaultProcess($driver);
        }
        while (true) {
            try {
                if ($process->isRunning()) {
                    continue;
                } else if (is_callable($this->onBeforeDispatch) && call_user_func_array($this->onBeforeDispatch, [$this, $process]) === false) {
                    // 跳过此次调度
                    continue;
                }
            } catch (\Throwable $e) {
                //
            }
            try {
                $process->run(function ($type, $data) {
                    echo $data;
                });
                $process->stop();
            } catch (\Throwable $e) {
                echo "[listener] " . $e->getMessage() . PHP_EOL;
            }
            try {
                if ($this->execInterval > 0) {
                    // 支持小数
                    usleep(intval($this->execInterval * 1000000));
                }
                if (is_callable($this->onAfterDispatch)) {
                    // 进程调度后
                    call_user_func_array($this->onAfterDispatch, [$this, $process]);
                }
            } catch (\Throwable $e) {
            }
        }
    }

    /**
     * 默认的队列消费进程
     * @param $driver
     * @param mixed $cwd
     * @return Process
     */
    protected function defaultProcess($driver, $cwd = null)
    {
        $command          = __DIR__ . '/stub/command/' . $driver . '.queue.php';
        $executableFinder = new PhpExecutableFinder();
        $php              = $executableFinder->find(false);
        $php              = false === $php ? null : array_merge([$php], $executableFinder->findArguments());
        return new Process([$php, $command], $cwd);
    }

    /**
     * 生成一个队列消费的进程
     * @param $process
     * @param null $cwd
     * @return $this
     */
    public function makeProcess($process, $cwd = null)
    {
        if ($process instanceof Process) {
            $this->process = $process;
        } else if (is_string($process)) {
            if ($this->makeAutoload) {
                $process = $this->makeAutoloadCode() . $process;
            }
            $this->process = new PhpProcess($process, $cwd);
        }
        return $this;
    }

    public function makeAutoloadCode()
    {
        $cwd = realpath(__DIR__ . '/../../../');
        return '<?php include \'' . $cwd . DIRECTORY_SEPARATOR . 'autoload.php\'; ?>';
    }

    /**
     * 快捷运行
     * @param $driver
     */
    static public function newRun($driver)
    {
        $instance = new static();
        $instance->run($driver);
    }
}
