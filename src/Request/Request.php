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
    private $requestId;


    public function __construct(string $requestId, int $chunkSize, int $chunkCount, array $data)
    {
        $this->chunkSize = $chunkSize;
        $this->chunkCount = $chunkCount;
        $this->data = $data;
        $this->requestId = $requestId;
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
        $consumer->addFinishedChunk($this->requestId, $chunkId, $message, $this);
    }

    public function removeFromContainer(Consumer $consumer): void
    {
        $consumer->removeCalculation($this->requestId);
    }

    public function getMessageData(): array
    {
        return $this->toArray();
    }

    private function toArray(): array
    {
        return [
            'requestId' => $this->requestId,
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
