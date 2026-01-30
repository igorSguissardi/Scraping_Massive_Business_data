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
        print(f"Log: {log}")
    
    print(f"\nCompanies discovered: {len(final_state['companies'])}")

if __name__ == "__main__":
    main()