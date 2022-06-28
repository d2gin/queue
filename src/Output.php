<?php

namespace icy8\queue;
class Output
{
    public function writeln($msg)
    {
        echo($msg . PHP_EOL);
    }

    public function info($msg)
    {
        $msg = '[info]' . date('Y-m-d H:i:s') . " {$msg}";
        $this->writeln($msg);
    }

    public function error($msg)
    {
        $msg = '[error]' . date('Y-m-d H:i:s') . " {$msg}";
        $this->writeln($msg);
    }
}