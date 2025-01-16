# Copyright 2024 Bloomberg Finance L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
from solution.producer_sol import mqProducer

def main():
    # Check if we have the right number of command line arguments
    if len(sys.argv) != 4:
        print("Usage: python publish.py <ticker> <price> <sector>")
        sys.exit(1)

    # Get values from command line
    ticker = sys.argv[1]
    price = sys.argv[2]
    sector = sys.argv[3]

    # Create routing key in format "stock.sector.ticker"
    routing_key = f"{sector}.{ticker}"
    
    # Create producer with topic exchange
    producer = mqProducer(
        routing_key=routing_key,
        exchange_name="stock_topic_exchange"  # New exchange name
    )
    
    # Create and send message
    message = f"{ticker} is ${price}"
    producer.publishOrder(message)

if __name__ == "__main__":
    main()