import json
import os


if __name__ == '__main__':
    print(json.dumps({f'peer{i}': os.urandom(32).hex() for i in range(1, 4)}))
