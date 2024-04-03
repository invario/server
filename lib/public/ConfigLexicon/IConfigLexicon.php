<?php

namespace OCP\ConfigLexicon;

interface IConfigLexicon {
	public function isStrict(): bool;

	/**
	 * @return IConfigLexiconEntry[]
	 */
	public function getAppConfigs(): array;
	/**
	 * @return IConfigLexiconEntry[]
	 */
	public function getUserPreferences(): array;
}
