<?php

namespace icy8\Queue;

use icy8\Queue\connector\Redis;
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

    public function run($driver = 'redis')
    {
        /* @var $process */
        $process = $this->process;
        if (!$process) {
            $process = $this->defaultProcess($driver);
        }
        while (true) {
            if ($process->isRunning()) continue;
            $process->run();
            $output = $process->getOutput();
            echo $output;
            $process->stop();
            if ($this->execInterval < 1) {
                usleep(intval($this->execInterval * 1000000));
            } else sleep($this->execInterval);
        }
    }

    /**
     * 默认的队列消费进程
     * @param $driver
     * @return PhpProcess
     */
    protected function defaultProcess($driver, $cwd = null)
    {
        $stub = file_get_contents(__DIR__ . '/stub/command/' . $driver . '.queue.php');
        $stub = $this->makeAutoloadCode() . $stub;
        return new PhpProcess($stub, $cwd);
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
