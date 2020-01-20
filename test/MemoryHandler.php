<?php

namespace Emartech\Chunkulator\Test;

use Monolog\Handler\AbstractHandler;
use Monolog\Logger;

class MemoryHandler extends AbstractHandler
{
    protected $logs = [];

    public function __construct($level = Logger::DEBUG, bool $bubble = true)
    {
        parent::__construct($level, $bubble);
    }

    public function handle(array $record): bool
    {
        if ($this->isHandling($record)) {
            $this->logs[] = $record;
        }
        return true;
    }

    public function getLogs(): array
    {
        return $this->logs;
    }
}
