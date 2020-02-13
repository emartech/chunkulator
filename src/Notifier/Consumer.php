<?php

namespace Emartech\Chunkulator\Notifier;

use Emartech\AmqpWrapper\Message;
use Emartech\AmqpWrapper\QueueConsumer;
use Emartech\Chunkulator\Request\Request;
use Emartech\Chunkulator\Request\ChunkRequestBuilder;
use Exception;
use Psr\Log\LoggerInterface;


class Consumer implements QueueConsumer
{
    private $resultHandler;
    private $logger;

    /** @var Calculation[] */
    private $calculations = [];


    public function __construct(ResultHandler $resultHandler, LoggerInterface $logger)
    {
        $this->resultHandler = $resultHandler;
        $this->logger = $logger;
    }

    public function getPrefetchCount(): ?int
    {
        return null;
    }

    public function consume(Message $message): void
    {
        $this->addMessage($message);
        try {
            $this->finishCalculations();
        } catch (Exception $ex) {
            $this->logger->error('Finishing calculation failed', ['exception' => $ex]);
            throw $ex;
        }
    }

    public function timeOut(): void
    {
        foreach ($this->calculations as $requestId => $calculation) {
            $calculation->defer();
            $this->removeCalculation($requestId);
        }
    }

    public function addMessage(Message $message): void
    {
        ChunkRequestBuilder::fromMessage($message)->addFinishedChunkTo($this, $message);
    }

    public function addFinishedChunk(string $requestId, int $chunkId, Message $message, Request $calculationRequest)
    {
        if (!isset($this->calculations[$requestId])) {
            $this->calculations[$requestId] = new Calculation($this->resultHandler, $calculationRequest);
        }
        $this->calculations[$requestId]->addFinishedChunk($chunkId, $message);
    }

    public function finishCalculations(): void
    {
        foreach ($this->calculations as $calculation) {
            $calculation->finish($this);
        }
    }

    public function removeCalculation(string $requestId): void
    {
        unset($this->calculations[$requestId]);
    }
}
