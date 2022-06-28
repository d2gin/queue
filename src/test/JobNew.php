<?php

namespace icy8\Queue\test;
class JobNew
{
    public function handle($job, $data)
    {
        var_dump("new job data2：");
        var_dump($data);
    }
}
