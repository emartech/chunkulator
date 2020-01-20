<?php

namespace Emartech\Chunkulator\Request;

use Emartech\AmqpWrapper\Message;
use Emartech\Chunkulator\Notifier\Consumer;

class Request
{
    const MAX_RETRY_COUNT = 3;

    public $chunkSize;
    private $chunkCount;
    private $data;
    private $triggerId;


    public function __construct(string $triggerId, int $chunkSize, int $chunkCount, array $data)
    {
        $this->chunkSize = $chunkSize;
        $this->chunkCount = $chunkCount;
        $this->data = $data;
        $this->triggerId = $triggerId;
    }

    public function getChunkOffset(int $chunkId): int
    {
        return $this->chunkSize * $chunkId;
    }

    public function allChunkIds(): array
    {
        return range(0, $this->chunkCount - 1);
    }

    public function addFinishedChunkTo(Consumer $consumer, Message $message, int $chunkId): void
    {
        $consumer->addFinishedChunk($this->triggerId, $chunkId, $message, $this);
    }

    public function removeFromContainer(Consumer $consumer): void
    {
        $consumer->removeCalculation($this->triggerId);
    }

    public function getMessageData(): array
    {
        return $this->toArray();
    }

    private function toArray(): array
    {
        return [
            'triggerId' => $this->triggerId,
            'chunkSize' => $this->chunkSize,
            'chunkCount' => $this->chunkCount,
            'data' => $this->data
        ];
    }

    public function getData(): array
    {
        return $this->data;
    }

    public function getChunkCount(): int
    {
        return $this->chunkCount;
    }
}
