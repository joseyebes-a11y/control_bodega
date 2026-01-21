# Pseudo-tests: capacidad consolidada en movimientos

1) Poblar `contenedores_estado` con `cantidad = 90` para un deposito con `capacidad_hl = 1` (100 L).
2) Llamar a `POST /api/movimientos` con destino a ese deposito y `litros = 15`.
3) Esperado: respuesta 400 con mensaje de capacidad excedida.
4) Llamar a `POST /api/movimientos` con `litros = 10`.
5) Esperado: OK (no supera capacidad).
