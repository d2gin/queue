<?php
$exec = new \icy8\Queue\Executor('redis');
$exec->getConnector()->init();
$exec->runNext();
