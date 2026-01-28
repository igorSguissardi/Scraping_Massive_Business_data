# Entry point for the application that triggers the graph execution with the required initial parameters.
from graph import app

def main():
    # The starting point is strictly the Valor 1000 URL provided in the challenge
    initial_input = {
        "initial_url": "https://infograficos.valor.globo.com/valor1000/rankings/ranking-das-1000-maiores/2025",
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