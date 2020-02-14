<?php

namespace Emartech\Chunkulator\Request;

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

    public function getRequestId(): string
    {
        return $this->requestId;
    }
}
