import os
import dotenv
dotenv.load_dotenv()
print(os.environ.get("ZIGMENT_API_KEY"))