<?php

namespace Emartech\Chunkulator\Calculator;

use Emartech\Chunkulator\Request\ChunkRequest;

interface ContactListHandler
{
    public function getContactsOfList(ChunkRequest $request): array;
    public function applyContactsToList(ChunkRequest $request, array $contactIds): void;
}
