# Manual check: aislamiento multi-tenant

Objetivo: comprobar que un usuario no ve ni edita datos de otra bodega.

1) Login como admin y crear dos usuarios

```sh
# login admin
curl -s -c /tmp/cookies-admin.txt \
  -H "Content-Type: application/json" \
  -d '{"usuario":"admin","password":"<ADMIN_PASSWORD>"}' \
  http://localhost:3001/login

# crear usuario A
curl -s -b /tmp/cookies-admin.txt \
  -H "Content-Type: application/json" \
  -d '{"usuario":"usuario_a","password":"clave_a","bodega_nombre":"Bodega A"}' \
  http://localhost:3001/api/admin/usuarios

# crear usuario B
curl -s -b /tmp/cookies-admin.txt \
  -H "Content-Type: application/json" \
  -d '{"usuario":"usuario_b","password":"clave_b","bodega_nombre":"Bodega B"}' \
  http://localhost:3001/api/admin/usuarios
```

2) Login como usuario A y crear un depósito

```sh
curl -s -c /tmp/cookies-a.txt \
  -H "Content-Type: application/json" \
  -d '{"usuario":"usuario_a","password":"clave_a"}' \
  http://localhost:3001/login

curl -s -b /tmp/cookies-a.txt \
  -H "Content-Type: application/json" \
  -d '{"codigo":"A-001"}' \
  http://localhost:3001/api/depositos

# obtener el ID de A-001
curl -s -b /tmp/cookies-a.txt \
  http://localhost:3001/api/depositos
```

3) Login como usuario B y comprobar que NO ve el depósito de A

```sh
curl -s -c /tmp/cookies-b.txt \
  -H "Content-Type: application/json" \
  -d '{"usuario":"usuario_b","password":"clave_b"}' \
  http://localhost:3001/login

curl -s -b /tmp/cookies-b.txt \
  http://localhost:3001/api/depositos
```

4) Intentar acceder al depósito de A con usuario B (debe devolver 404)

```sh
# usa el id devuelto en el paso 2 como <ID_A1>
curl -i -s -b /tmp/cookies-b.txt \
  http://localhost:3001/api/depositos/<ID_A1>
```

Resultado esperado:
- La lista para usuario B NO contiene el depósito `A-001`.
- El GET al ID de A devuelve 404.
