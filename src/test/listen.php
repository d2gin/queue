<?php

use icy8\Queue\Listener;
use Symfony\Component\Process\PhpProcess;

include __DIR__ . "/../../../../autoload.php";
$listener = new Listener();
// 进程调度间隔 单位秒 默认是1秒 允许小数
$listener->execInterval = '0.1';
// 定制自己的消费进程
/*$listener->makeProcess('<?php
// 为redis配置密码
$exec = new \icy8\Queue\Executor("redis", ["port"=>"6399", "password"=>"123456"]);
$exec->runNext();');*/
// 开始监听队列
$listener->run();