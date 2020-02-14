<?php

namespace Emartech\Chunkulator\Notifier;

use Emartech\AmqpWrapper\Message;
use Emartech\AmqpWrapper\QueueConsumer;
use Emartech\Chunkulator\Request\ChunkRequest;
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
        $chunkRequest = ChunkRequestBuilder::fromMessage($message);
        $calculation = $this->getCalculation($chunkRequest);
        $calculation->addFinishedChunk($chunkRequest->getChunkId(), $message);
        try {
            $calculation->finish($this);
        } catch (Exception $ex) {
            $this->logger->error('Finishing calculation failed', ['exception' => $ex]);
            throw $ex;
        }
    }

    public function timeOut(): void
    {
        foreach ($this->calculations as $requestId => $calculation) {
            $calculation->requeue();
            $this->removeCalculation($requestId);
        }
    }

    public function removeCalculation(string $requestId): void
    {
        unset($this->calculations[$requestId]);
    }

    private function getCalculation(ChunkRequest $chunkRequest)
    {
        $calculationRequest = $chunkRequest->getCalculationRequest();
        $requestId = $calculationRequest->getRequestId();
        if (!isset($this->calculations[$requestId])) {
            $this->calculations[$requestId] = new Calculation($this->resultHandler, $calculationRequest);
        }
        return $this->calculations[$requestId];
    }
}
