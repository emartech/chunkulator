<?php

namespace Emartech\Chunkulator\Request;

use Emartech\AmqpWrapper\Message;
use Emartech\AmqpWrapper\Queue;
use Emartech\Chunkulator\Notifier\Consumer;

class ChunkRequest
{
    private $chunkId;
    public $tries;
    private $calculationRequest;


    public function __construct(Request $calculationRequest, int $chunkId, int $tries)
    {
        $this->calculationRequest = $calculationRequest;
        $this->chunkId = $chunkId;
        $this->tries = $tries;
    }

    private function toArray(): array
    {
        return $this->calculationRequest->getMessageData() + [
            'chunkId' => $this->chunkId,
            'tries' => $this->tries,
        ];
    }

    public function enqueueIn(Queue $queue): void
    {
        $queue->send($this->getMessageData());
    }

    public function addFinishedChunkTo(Consumer $container, Message $message)
    {
        $this->calculationRequest->addFinishedChunkTo($container, $message, $this->chunkId);
    }

    public function getMessageData(): array
    {
        return $this->toArray();
    }

    public function getCalculationRequest(): Request
    {
        return $this->calculationRequest;
    }

    public function getChunkId(): int
    {
        return $this->chunkId;
    }
}
