# Entry point for the application that triggers the graph execution with the required initial parameters.
from dotenv import load_dotenv
from graph import app

# Load environment variables from .env file at application startup
load_dotenv()

def main(): 
    # The starting point is strictly the Valor 1000 URL provided in the challenge
    initial_input = {
        "initial_url": "https://infovalorbucket.s3.amazonaws.com/arquivos/valor-1000/2025/ranking-das-1000-maiores/RankingValor10002025.json?0.39000846525186394",
        "companies": [],
        "execution_logs": []
    }
    
    print("--- Starting Valor 1000 Intelligence Discovery ---")
    
    # Run the graph and capture the final state
    final_state = app.invoke(initial_input)
    
    # Display the result of the intelligence process
    print("\n--- Final Process Logs ---")
    for log in final_state["execution_logs"]:
        print(f"  {log}")
    
    # Show enriched companies details
    enriched_companies = [c for c in final_state["companies"] if c.get("official_website") or c.get("primary_cnpj")]
    
    if enriched_companies:
        print("\n--- Enriched Companies Summary ---")
        for company in enriched_companies:
            nome = company.get("nome_empresa", "N/A")
            sede = company.get("sede", "N/A")
            website = company.get("official_website", "❌ Not found")
            cnpj = company.get("primary_cnpj", "❌ Not found")
            corporate = company.get("corporate_group_notes", "No info")
            brands = company.get("found_brands", [])
            
            print(f"\n  📊 {nome} | {sede}")
            print(f"     🌐 Website: {website}")
            print(f"     📋 CNPJ: {cnpj}")
            if corporate:
                print(f"     🏢 Corporate Info: {corporate}")
            if brands:
                print(f"     🏷️  Brands: {', '.join(brands)}")
    
    print(f"\n✅ Total companies in database: {len(final_state['companies'])}")
    print(f"✅ Enriched companies: {len(enriched_companies)}")
    print(f"✅ Pending enrichment: {len(final_state['companies']) - len(enriched_companies)}")

if __name__ == "__main__":
    main()