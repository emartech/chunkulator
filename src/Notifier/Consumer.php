<?php

namespace Emartech\Chunkulator\Notifier;

use Emartech\AmqpWrapper\Message;
use Emartech\AmqpWrapper\QueueConsumer;
use Emartech\Chunkulator\QueueFactory;
use Emartech\Chunkulator\Request\ChunkRequest;
use Emartech\Chunkulator\Request\ChunkRequestBuilder;
use Exception;
use Emartech\Chunkulator\Exception as ResultHandlerException;
use Psr\Log\LoggerInterface;
use Throwable;


class Consumer implements QueueConsumer
{
    private $resultHandler;
    private $logger;
    private $queueFactory;

    /** @var Calculation[] */
    private $calculations = [];

    public function __construct(ResultHandler $resultHandler, LoggerInterface $logger, QueueFactory $queueFactory)
    {
        $this->resultHandler = $resultHandler;
        $this->logger = $logger;
        $this->queueFactory = $queueFactory;
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
        } catch (ResultHandlerException $ex) {
            $this->logger->error('Finishing calculation failed', ['exception' => $ex]);
            $this->retry($calculation, $chunkRequest);
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

    private function retry(Calculation $calculation, ChunkRequest $request): void
    {
        $notifierQueue = $this->queueFactory->createNotifierQueue();
        if ($request->tries > 0) {
            $request->tries--;
            $request->enqueueIn($notifierQueue);
            $calculation->discardChunk($request->getChunkId());
        } else {
            try {
                $this->resultHandler->onFailure($request->getCalculationRequest()->getData());
                $calculation->discard();
            } catch (Throwable $t) {
                $request->enqueueIn($notifierQueue);
                $calculation->discardChunk($request->getChunkId());
                throw $t;
            }
        }
    }
}
