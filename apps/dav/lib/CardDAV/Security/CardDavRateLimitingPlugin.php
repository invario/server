<?php

declare(strict_types=1);

/*
 * @copyright 2023 Christoph Wurst <christoph@winzerhof-wurst.at>
 *
 * @author 2023 Christoph Wurst <christoph@winzerhof-wurst.at>
 *
 * @license GNU AGPL version 3 or any later version
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

namespace OCA\DAV\CardDAV\Security;

use OC\Security\RateLimiting\Exception\RateLimitExceededException;
use OC\Security\RateLimiting\Limiter;
use OCA\DAV\CardDAV\CardDavBackend;
use OCA\DAV\Connector\Sabre\Exception\TooManyRequests;
use OCP\IAppConfig;
use OCP\IUserManager;
use Psr\Log\LoggerInterface;
use Sabre\DAV;
use Sabre\DAV\Exception\Forbidden;
use Sabre\DAV\ServerPlugin;
use function count;
use function explode;

class CardDavRateLimitingPlugin extends ServerPlugin {

	private Limiter $limiter;
	private IUserManager $userManager;
	private CardDavBackend $cardDavBackend;
	private IAppConfig $config;
	private LoggerInterface $logger;
	private ?string $userId;

	public function __construct(Limiter $limiter,
		IUserManager $userManager,
		CardDavBackend $cardDavBackend,
		LoggerInterface $logger,
		IAppConfig $config,
		?string $userId) {
		$this->limiter = $limiter;
		$this->userManager = $userManager;
		$this->cardDavBackend = $cardDavBackend;
		$this->config = $config;
		$this->logger = $logger;
		$this->userId = $userId;
	}

	public function initialize(DAV\Server $server): void {
		$server->on('beforeBind', [$this, 'beforeBind'], 1);
	}

	public function beforeBind(string $path): void {
		if ($this->userId === null) {
			// We only care about authenticated users here
			return;
		}
		$user = $this->userManager->get($this->userId);
		if ($user === null) {
			// We only care about authenticated users here
			return;
		}

		$pathParts = explode('/', $path);
		if (count($pathParts) === 4 && $pathParts[0] === 'addressbooks') {
			// Path looks like addressbooks/users/username/addressbooksname so a new addressbook is created
			try {
				$this->limiter->registerUserRequest(
					'carddav-create-address-book',
					$this->config->getValueInt('dav', 'rateLimitAddressBookCreation', 10),
					$this->config->getValueInt('dav', 'rateLimitPeriodAddressBookCreation', 3600),
					$user
				);
			} catch (RateLimitExceededException $e) {
				throw new TooManyRequests('Too many calendars created', 0, $e);
			}

			$addressBookLimit = $this->config->getValueInt('dav', 'maximumAdressbooks', 10);
			if ($addressBookLimit === -1) {
				return;
			}
			$numAddressbooks = $this->cardDavBackend->getAddressBooksForUserCount('principals/users/' . $user->getUID());

			if ($numAddressbooks >= $addressBookLimit) {
				$this->logger->warning('Maximum number of address books reached', [
					'addressbooks' => $numAddressbooks,
					'addressBookLimit' => $addressBookLimit,
				]);
				throw new Forbidden('AddressBook limit reached', 0);
			}
		}
	}

}
