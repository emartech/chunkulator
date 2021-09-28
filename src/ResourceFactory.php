<?php

namespace Emartech\Chunkulator;

use Emartech\Chunkulator\Calculator\ContactListHandler;
use Emartech\Chunkulator\Calculator\Filter as FilterInterface;
use Emartech\Chunkulator\ResultHandler;

interface ResourceFactory
{
    public function createQueueFactory(): QueueFactory;

    public function createFilter(): FilterInterface;

    public function createResultHandler(): ResultHandler;

    public function createContactListHandler(): ContactListHandler;
}
