#!/bin/bash

echo "=========================================="
echo "   DispoMinera - Sistema de Monitoreo    "
echo "=========================================="
echo ""
echo "Iniciando aplicación..."
echo ""
echo "La aplicación estará disponible en:"
echo "http://localhost:5000"
echo ""
echo "Presiona Ctrl+C para detener el servidor"
echo ""

cd /home/claude/dispominera
python3 app.py
