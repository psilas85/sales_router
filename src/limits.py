# /home/ubuntu/sales_router/src/limits.py
#
# Limites operacionais compartilhados entre módulos.
# Mantém setorização e roteirização PAREADAS — toda a setorização é
# carregada inteira pela roteirização, então os dois precisam aceitar o
# mesmo teto. Se subir aqui, sobe nos dois lugares de uma vez só.

MAX_PDVS_POR_EXECUCAO = 3000
