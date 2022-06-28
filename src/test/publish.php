<?php

include __DIR__ . "/../../../../autoload.php";
$connection = new \icy8\Queue\connector\Redis();
$connection->init();
// 任务立即执行
$connection->push(\icy8\Queue\test\JobTest::class, ['a', 'b', 'c']);
// 运行任务类的自定义方法
$connection->push([\icy8\Queue\test\JobTest::class, 'custom'], '自定义方法');
// 延迟任务
$connection->pushDelay(\icy8\Queue\test\JobTest::class, ['a', 'b', 'c'], 3);
