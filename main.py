# Entry point for the application that triggers the graph execution with the required initial parameters.
import os
import asyncio
import time
from dotenv import load_dotenv

# Load environment variables BEFORE importing anything else that needs them
# Explicitly specify the .env file path
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
print(f"[DEBUG] Loading .env from: {env_path}")
print(f"[DEBUG] File exists: {os.path.exists(env_path)}")

load_dotenv(dotenv_path=env_path, override=True, verbose=True)

# Verify API key is loaded
api_key = os.getenv("OPENAI_API_KEY")
if api_key:
    print(f"[DEBUG] ✓ OPENAI_API_KEY loaded successfully (length: {len(api_key)})")
else:
    print("[DEBUG] ✗ OPENAI_API_KEY not found after load_dotenv()")

from graph import app

def main(): 
    # The starting point is strictly the Valor 1000 URL provided in the challenge
    initial_input = {
        "initial_url": "https://infovalorbucket.s3.amazonaws.com/arquivos/valor-1000/2025/ranking-das-1000-maiores/RankingValor10002025.json?0.39000846525186394",
        "companies": [],
        "company_queue": [],
        "execution_logs": [],
        "institutional_markdown": [],
        "institutional_summary": [],
        "corporate_csv_evidence": [],
        "ingested_company_ids": [],
        "llm_request_count": 0,
    }
    
    print("--- Starting Valor 1000 Intelligence Discovery ---")
    
    # Run the graph and capture the final state
    run_start = time.perf_counter()
    final_state = asyncio.run(app.ainvoke(initial_input))
    run_end = time.perf_counter()
    run_elapsed_seconds = run_end - run_start
    
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
    print(f"✅ Total LLM API requests (run): {final_state.get('llm_request_count', 0)}")
    print(f"✅ Run duration (seconds): {run_elapsed_seconds:.2f}")

if __name__ == "__main__":
    main()
