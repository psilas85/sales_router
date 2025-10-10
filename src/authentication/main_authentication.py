# sales_router/src/authentication/main_authentication.py

import sys, os, argparse
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from authentication.use_case.tenant_use_case import TenantUseCase
from authentication.use_case.user_use_case import UserUseCase
from authentication.entities.tenant import Tenant
from authentication.entities.user import User

def main():
    parser = argparse.ArgumentParser(description="Módulo de Autenticação - SalesRouter")

    parser.add_argument("--action", required=True,
                        choices=["init", "create_tenant", "create_user", "login"],
                        help="Ação a ser executada.")

    parser.add_argument("--razao", help="Razão social do tenant")
    parser.add_argument("--fantasia", help="Nome fantasia do tenant")
    parser.add_argument("--cnpj", help="CNPJ do tenant")
    parser.add_argument("--email", help="E-mail do tenant ou usuário")
    parser.add_argument("--senha", help="Senha do usuário")
    parser.add_argument("--tenant_id", type=int, help="ID do tenant (para criar usuário)")
    parser.add_argument("--nome", help="Nome do usuário")
    parser.add_argument("--role", choices=["tenant_adm", "tenant_operacional"], help="Tipo de usuário a criar")

    args = parser.parse_args()
    tenant_uc = TenantUseCase()
    user_uc = UserUseCase()

    if args.action == "init":
        print("🚀 Inicializando estrutura de autenticação...")
        tenant_uc.setup_table()
        user_uc.setup_table()

        tenant = tenant_uc.create_master_tenant()
        print(f"🏢 Tenant Master criado com ID: {tenant.id}")

        user = user_uc.create_sales_router_admin(tenant.id)
        print(f"👤 Usuário SalesRouter Admin criado com ID: {user.id}")

        token = user_uc.login("admin@salesrouter.com", "admin123")
        print(f"🔐 Token JWT: {token}")

    elif args.action == "create_tenant":
        if not all([args.razao, args.cnpj, args.email]):
            print("❌ Faltam parâmetros obrigatórios: --razao, --cnpj, --email")
            return
        tenant = Tenant(
            razao_social=args.razao,
            nome_fantasia=args.fantasia or args.razao,
            cnpj=args.cnpj,
            email_adm=args.email,
            is_master=False
        )
        tenant = tenant_uc.repo.create(tenant)
        print(f"✅ Tenant '{tenant.nome_fantasia}' criado com ID: {tenant.id}")

    elif args.action == "create_user":
        if not all([args.tenant_id, args.nome, args.email, args.senha, args.role]):
            print("❌ Faltam parâmetros obrigatórios: --tenant_id, --nome, --email, --senha, --role")
            return

        if args.role == "tenant_adm":
            user = user_uc.create_tenant_admin(args.tenant_id, args.nome, args.email, args.senha)
        else:
            user = user_uc.create_tenant_operacional(args.tenant_id, args.nome, args.email, args.senha)
        print(f"✅ Usuário '{user.nome}' ({user.role}) criado com ID: {user.id}")

    elif args.action == "login":
        if not all([args.email, args.senha]):
            print("❌ Faltam parâmetros obrigatórios: --email, --senha")
            return
        token = user_uc.login(args.email, args.senha)
        if token:
            print(f"🔐 Login bem-sucedido. Token JWT: {token}")
        else:
            print("❌ Falha no login: credenciais inválidas.")

if __name__ == "__main__":
    main()
