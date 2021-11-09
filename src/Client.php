<?php
// +----------------------------------------------------------------------
// | CodeEngine
// +----------------------------------------------------------------------
// | Copyright 艾邦
// +----------------------------------------------------------------------
// | Licensed ( http://www.apache.org/licenses/LICENSE-2.0 )
// +----------------------------------------------------------------------
// | Author: TaoGe <liangtao.gz@foxmail.com>
// +----------------------------------------------------------------------
// | Date: 2021/11/9 14:29
// +----------------------------------------------------------------------

namespace Etcd;

use GuzzleHttp\Client as HttpClient;

class Client
{
    // KV
    const URI_PUT          = 'kv/put';
    const URI_RANGE        = 'kv/range';
    const URI_DELETE_RANGE = 'kv/deleterange';
    const URI_TXN          = 'kv/txn';
    const URI_COMPACTION   = 'kv/compaction';

    // Lease
    const URI_GRANT      = 'lease/grant';
    const URI_REVOKE     = 'kv/lease/revoke';
    const URI_KEEPALIVE  = 'lease/keepalive';
    const URI_TIMETOLIVE = 'kv/lease/timetolive';

    // Role
    const URI_AUTH_ROLE_ADD    = 'auth/role/add';
    const URI_AUTH_ROLE_GET    = 'auth/role/get';
    const URI_AUTH_ROLE_DELETE = 'auth/role/delete';
    const URI_AUTH_ROLE_LIST   = 'auth/role/list';

    // Authenticate
    const URI_AUTH_ENABLE       = 'auth/enable';
    const URI_AUTH_DISABLE      = 'auth/disable';
    const URI_AUTH_AUTHENTICATE = 'auth/authenticate';

    // User
    const URI_AUTH_USER_ADD             = 'auth/user/add';
    const URI_AUTH_USER_GET             = 'auth/user/get';
    const URI_AUTH_USER_DELETE          = 'auth/user/delete';
    const URI_AUTH_USER_CHANGE_PASSWORD = 'auth/user/changepw';
    const URI_AUTH_USER_LIST            = 'auth/user/list';

    const URI_AUTH_ROLE_GRANT  = 'auth/role/grant';
    const URI_AUTH_ROLE_REVOKE = 'auth/role/revoke';

    const URI_AUTH_USER_GRANT  = 'auth/user/grant';
    const URI_AUTH_USER_REVOKE = 'auth/user/revoke';

    const PERMISSION_READ      = 0;
    const PERMISSION_WRITE     = 1;
    const PERMISSION_READWRITE = 2;

    const DEFAULT_HTTP_TIMEOUT = 30;

    /**
     * @var string host:port
     */
    protected string $server;
    /**
     * @var string api version
     */
    protected string $version;

    /**
     * @var array
     */
    protected array $httpOptions;

    /**
     * @var bool
     */
    protected bool $pretty = false;

    /**
     * @var string|null auth token
     */
    protected ?string $token = null;

    public function __construct($server = '127.0.0.1:2379', $version = 'v3')
    {
        $this->server = rtrim($server, "/");
        if (!str_starts_with($this->server, 'http')) {
            /** @noinspection HttpUrlsUsage */
            $this->server = 'http://' . $this->server;
        }
        $this->version = trim($version);
    }

    public function setHttpOptions(array $options)
    {
        $this->httpOptions = $options;
    }

    public function setPretty($enabled)
    {
        $this->pretty = $enabled;
    }

    public function setToken($token)
    {
        $this->token = $token;
    }

    public function clearToken()
    {
        $this->token = null;
    }

    // region kv

    /**
     * Put puts the given key into the key-value store.
     * A put request increments the revision of the key-value
     * store\nand generates one event in the event history.
     * @param string $key
     * @param string $value
     * @param array  $options 可选参数
     *                        int64  lease
     *                        bool   prev_kv
     *                        bool   ignore_value
     *                        bool   ignore_lease
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function put(string $key, string $value, array $options = []): array
    {
        $params = [
            'key'   => $key,
            'value' => $value,
        ];

        $params  = $this->encode($params);
        $options = $this->encode($options);
        $body    = $this->request(self::URI_PUT, $params, $options);
        $body    = $this->decodeBodyForFields(
            $body,
            'prev_kv',
            ['key', 'value',]
        );

        if (isset($body['prev_kv']) && $this->pretty) {
            return $this->convertFields($body['prev_kv']);
        }

        return $body;
    }

    /**
     * Gets the key or a range of keys
     * @param string $key
     * @param array  $options
     *         string range_end
     *         int    limit
     *         int    revision
     *         int    sort_order
     *         int    sort_target
     *         bool   serializable
     *         bool   keys_only
     *         bool   count_only
     *         int64  min_mod_revision
     *         int64  max_mod_revision
     *         int64  min_create_revision
     *         int64  max_create_revision
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function get(string $key, array $options = []): array
    {
        $params  = [
            'key' => $key,
        ];
        $params  = $this->encode($params);
        $options = $this->encode($options);
        $body    = $this->request(self::URI_RANGE, $params, $options);
        $body    = $this->decodeBodyForFields(
            $body,
            'kvs',
            ['key', 'value',]
        );

        if (isset($body['kvs']) && $this->pretty) {
            return $this->convertFields($body['kvs']);
        }

        return $body;
    }

    /**
     * get all keys
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function getAllKeys(): array
    {
        return $this->get("\0", ['range_end' => "\0"]);
    }

    /**
     * get all keys with prefix
     * @param string $prefix
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function getKeysWithPrefix(string $prefix): array
    {
        $prefix = trim($prefix);
        if (!$prefix) {
            return [];
        }
        $lastIndex            = strlen($prefix) - 1;
        $lastChar             = $prefix[$lastIndex];
        $nextAsciiCode        = ord($lastChar) + 1;
        $rangeEnd             = $prefix;
        $rangeEnd[$lastIndex] = chr($nextAsciiCode);

        return $this->get($prefix, ['range_end' => $rangeEnd]);
    }

    /**
     * Removes the specified key or range of keys
     * @param string $key
     * @param array  $options
     *        string range_end
     *        bool   prev_kv
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function del(string $key, array $options = []): array
    {
        $params  = [
            'key' => $key,
        ];
        $params  = $this->encode($params);
        $options = $this->encode($options);
        $body    = $this->request(self::URI_DELETE_RANGE, $params, $options);
        $body    = $this->decodeBodyForFields(
            $body,
            'prev_kvs',
            ['key', 'value',]
        );

        if (isset($body['prev_kvs']) && $this->pretty) {
            return $this->convertFields($body['prev_kvs']);
        }

        return $body;
    }

    /**
     * Compact compacts the event history in the etcd key-value store.
     * The key-value\nstore should be periodically compacted
     * or the event history will continue to grow\nindefinitely.
     * @param int        $revision
     * @param bool|false $physical
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function compaction(int $revision, bool $physical = false): array
    {
        $params = [
            'revision' => $revision,
            'physical' => $physical,
        ];

        return $this->request(self::URI_COMPACTION, $params);
    }

    // endregion kv

    // region lease

    /**
     * LeaseGrant creates a lease which expires if the server does not receive a
     * keepAlive\nwithin a given time to live period. All keys attached to the lease
     * will be expired and\ndeleted if the lease expires.
     * Each expired key generates a delete event in the event history.",
     * @param int $ttl    TTL is the advisory time-to-live in seconds.
     * @param int $id     ID is the requested ID for the lease.
     *                    If ID is set to 0, the lessor chooses an ID.
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function grant(int $ttl, int $id = 0): array
    {
        $params = [
            'TTL' => $ttl,
            'ID'  => $id,
        ];

        return $this->request(self::URI_GRANT, $params);
    }

    /**
     * revokes a lease. All keys attached to the lease will expire and be deleted.
     * @param int $id ID is the lease ID to revoke. When the ID is revoked,
     *                all associated keys will be deleted.
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function revoke(int $id): array
    {
        $params = [
            'ID' => $id,
        ];

        return $this->request(self::URI_REVOKE, $params);
    }

    /**
     * keeps the lease alive by streaming keep alive requests
     * from the client\nto the server and streaming keep alive responses
     * from the server to the client.
     * @param int $id ID is the lease ID for the lease to keep alive.
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function keepAlive(int $id): array
    {
        $params = [
            'ID' => $id,
        ];

        $body = $this->request(self::URI_KEEPALIVE, $params);

        if (!isset($body['result'])) {
            return $body;
        }
        // response "result" field, etcd bug?
        return [
            'ID'  => $body['result']['ID'],
            'TTL' => $body['result']['TTL'],
        ];
    }

    /**
     * retrieves lease information.
     * @param int        $id ID is the lease ID for the lease.
     * @param bool|false $keys
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function timeToLive(int $id, bool $keys = false): array
    {
        $params = [
            'ID'   => $id,
            'keys' => $keys,
        ];

        $body = $this->request(self::URI_TIMETOLIVE, $params);

        if (isset($body['keys'])) {
            $body['keys'] = array_map(function ($value) {
                return base64_decode($value);
            }, $body['keys']);
        }

        return $body;
    }

    // endregion lease

    // region auth

    /**
     * enable authentication
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function authEnable(): array
    {
        $body = $this->request(self::URI_AUTH_ENABLE);
        $this->clearToken();

        return $body;
    }

    /**
     * disable authentication
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function authDisable(): array
    {
        $body = $this->request(self::URI_AUTH_DISABLE);
        $this->clearToken();

        return $body;
    }

    /**
     * @param string $user
     * @param string $password
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function authenticate(string $user, string $password): array
    {
        $params = [
            'name'     => $user,
            'password' => $password,
        ];

        $body = $this->request(self::URI_AUTH_AUTHENTICATE, $params);
        if ($this->pretty && isset($body['token'])) {
            return $body['token'];
        }

        return $body;
    }

    /**
     * add a new role.
     * @param string $name
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function addRole(string $name): array
    {
        $params = [
            'name' => $name,
        ];

        return $this->request(self::URI_AUTH_ROLE_ADD, $params);
    }

    /**
     * get detailed role information.
     * @param string $role
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function getRole(string $role): array
    {
        $params = [
            'role' => $role,
        ];

        $body = $this->request(self::URI_AUTH_ROLE_GET, $params);
        $body = $this->decodeBodyForFields(
            $body,
            'perm',
            ['key', 'range_end',]
        );
        if ($this->pretty && isset($body['perm'])) {
            return $body['perm'];
        }

        return $body;
    }

    /**
     * delete a specified role.
     * @param string $role
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function deleteRole(string $role): array
    {
        $params = [
            'role' => $role,
        ];

        return $this->request(self::URI_AUTH_ROLE_DELETE, $params);
    }

    /**
     * get lists of all roles
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function roleList(): array
    {
        $body = $this->request(self::URI_AUTH_ROLE_LIST);

        if ($this->pretty && isset($body['roles'])) {
            return $body['roles'];
        }

        return $body;
    }

    /**
     * add a new user
     * @param string $user
     * @param string $password
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function addUser(string $user, string $password): array
    {
        $params = [
            'name'     => $user,
            'password' => $password,
        ];

        return $this->request(self::URI_AUTH_USER_ADD, $params);
    }

    /**
     * get detailed user information
     * @param string $user
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function getUser(string $user): array
    {
        $params = [
            'name' => $user,
        ];

        $body = $this->request(self::URI_AUTH_USER_GET, $params);
        if ($this->pretty && isset($body['roles'])) {
            return $body['roles'];
        }

        return $body;
    }

    /**
     * delete a specified user
     * @param string $user
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function deleteUser(string $user): array
    {
        $params = [
            'name' => $user,
        ];

        return $this->request(self::URI_AUTH_USER_DELETE, $params);
    }

    /**
     * get a list of all users.
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function userList(): array
    {
        $body = $this->request(self::URI_AUTH_USER_LIST);
        if ($this->pretty && isset($body['users'])) {
            return $body['users'];
        }

        return $body;
    }

    /**
     * change the password of a specified user.
     * @param string $user
     * @param string $password
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function changeUserPassword(string $user, string $password): array
    {
        $params = [
            'name'     => $user,
            'password' => $password,
        ];

        return $this->request(self::URI_AUTH_USER_CHANGE_PASSWORD, $params);
    }

    /**
     * grant a permission of a specified key or range to a specified role.
     * @param string      $role
     * @param int         $permType
     * @param string      $key
     * @param string|null $rangeEnd
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function grantRolePermission(string $role, int $permType, string $key, string $rangeEnd = null): array
    {
        $params = [
            'name' => $role,
            'perm' => [
                'permType' => $permType,
                'key'      => base64_encode($key),
            ],
        ];
        if ($rangeEnd !== null) {
            $params['perm']['range_end'] = base64_encode($rangeEnd);
        }

        return $this->request(self::URI_AUTH_ROLE_GRANT, $params);
    }

    /**
     * revoke a key or range permission of a specified role.
     * @param string      $role
     * @param string      $key
     * @param string|null $rangeEnd
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function revokeRolePermission(string $role, string $key, string $rangeEnd = null): array
    {
        $params = [
            'role' => $role,
            'key'  => $key,
        ];
        if ($rangeEnd !== null) {
            $params['range_end'] = $rangeEnd;
        }

        return $this->request(self::URI_AUTH_ROLE_REVOKE, $params);
    }

    /**
     * grant a role to a specified user.
     * @param string $user
     * @param string $role
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function grantUserRole(string $user, string $role): array
    {
        $params = [
            'user' => $user,
            'role' => $role,
        ];

        return $this->request(self::URI_AUTH_USER_GRANT, $params);
    }

    /**
     * revoke a role of specified user.
     * @param string $user
     * @param string $role
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function revokeUserRole(string $user, string $role): array
    {
        $params = [
            'name' => $user,
            'role' => $role,
        ];

        return $this->request(self::URI_AUTH_USER_REVOKE, $params);
    }

    // endregion auth

    /**
     * 发送HTTP请求
     * @param string $uri
     * @param array  $params  请求参数
     * @param array  $options 可选参数
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    protected function request(string $uri, array $params = [], array $options = []): array
    {
        if ($options) {
            $params = array_merge($params, $options);
        }
        // 没有参数, 设置一个默认参数
        if (!$params) {
            $params['php-etcd-client'] = 1;
        }
        $data = [
            'json' => $params,
        ];
        if ($this->token) {
            $data['headers'] = ['Grpc-Metadata-Token' => $this->token];
        }

        $response = $this->getHttpClient()->request('post', $uri, $data);
        $content  = $response->getBody()->getContents();

        $body = json_decode($content, true);
        if ($this->pretty && isset($body['header'])) {
            unset($body['header']);
        }

        return $body;
    }

    protected function getHttpClient()
    {
        static $httpClient = null;
        if ($httpClient !== null) {
            return $httpClient;
        }
        $baseUri                       = sprintf('%s/%s/', $this->server, $this->version);
        $this->httpOptions['base_uri'] = $baseUri;
        if (!array_key_exists('timeout', $this->httpOptions)) {
            $this->httpOptions['timeout'] = self::DEFAULT_HTTP_TIMEOUT;
        }
        return new HttpClient($this->httpOptions);
    }

    /**
     * string类型key用base64编码
     * @param array $data
     * @return array
     */
    protected function encode(array $data): array
    {

        foreach ($data as $key => $value) {
            if (is_string($value)) {
                $data[$key] = base64_encode($value);
            }
        }

        return $data;
    }

    /**
     * 指定字段base64解码
     * @param array  $body
     * @param string $bodyKey
     * @param array  $fields 需要解码的字段
     * @return array
     */
    protected function decodeBodyForFields(array $body, string $bodyKey, array $fields): array
    {
        if (!isset($body[$bodyKey])) {
            return $body;
        }
        $data = $body[$bodyKey];
        if (!isset($data[0])) {
            $data = array($data);
        }
        foreach ($data as $key => $value) {
            foreach ($fields as $field) {
                if (isset($value[$field])) {
                    $data[$key][$field] = base64_decode($value[$field]);
                }
            }
        }

        if (isset($body[$bodyKey][0])) {
            $body[$bodyKey] = $data;
        } else {
            $body[$bodyKey] = $data[0];
        }

        return $body;
    }

    protected function convertFields(array $data)
    {
        if (!isset($data[0])) {
            return $data['value'];
        }

        $map = [];
        foreach ($data as $value) {
            $key       = $value['key'];
            $map[$key] = $value['value'];
        }

        return $map;
    }
}
