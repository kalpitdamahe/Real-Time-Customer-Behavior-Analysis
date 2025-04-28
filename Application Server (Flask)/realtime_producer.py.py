from flask import Flask, Response, render_template
import random
import time
import json
from datetime import datetime, timedelta

app = Flask(__name__)

products = {
    "p001": "Electronics",
    "p002": "Books",
    "p003": "Clothing",
    "p004": "Home & Kitchen",
    "p005": "Sports",
    "p006": "Toys"
}

# List of possible campaign codes
campaigns = ["CAM001", "CAM002", "CAM003", "CAM004", "CAM005"]

# List of possible payment methods
payment_methods = ["pm_visa", "pm_mastercard", "pm_paypal", "pm_amex"]

# Function to generate random timestamp
def random_timestamp(base_time, offset_minutes=0):
    offset = random.randint(1, 10) + offset_minutes  # Random minutes offset
    return (base_time + timedelta(minutes=offset)).isoformat() + "Z"

# Function to generate a random session_id (e.g., a random string)
def generate_session_id(user_id):
    return f"session_{user_id:06d}_{random.randint(1000, 9999)}"

# Function to assign a campaign to a user (optional)
def assign_campaign():
    # 30% chance that a user is part of a campaign
    if random.random() < 0.3:
        return random.choice(campaigns)
    return None  # No campaign for the remaining users

# Function to generate a random price per product (for total_price calculation)
def generate_price():
    return round(random.uniform(10, 500), 2)  # Random price between $10 and $500


def data_generator():
    base_time = datetime(2024, 10, 29, 9, 0)  # Starting point for timestamps
    data = []
    num_users = 100000
    max_actions_per_user = 5

    for user_id in range(1, num_users + 1):
        num_actions = random.randint(1, max_actions_per_user)  # Random actions for each user
        session_id = generate_session_id(user_id)  # Generate a session ID for the user
        campaign_code = assign_campaign()  # Assign a campaign code to the user (optional)
        viewed_products = set()  # Track products the user has viewed
        carted_products = set()  # Track products the user has added to the cart

        for _ in range(num_actions):
            action_type = random.choices(
                ["view", "add_to_cart", "purchase"],
                weights=[0.4, 0.3, 0.3],  # Adjusted probabilities for direct purchase
                k=1
            )[0]

            if action_type == "view" or not viewed_products:
                # View action
                product_id = random.choice(list(products.keys()))
                category = products[product_id]
                timestamp = random_timestamp(base_time)

                viewed_products.add(product_id)  # Mark this product as viewed

                entry = {
                    "user_id": f"u{user_id:06d}",  # Format user_id to be zero-padded
                    "session_id": session_id,  # Include the session ID
                    "campaign_code": campaign_code,  # Include the campaign code (if any)
                    "action_type": "view",
                    "product_id": product_id,
                    "category": category,
                    "timestamp": timestamp
                }

                data.append(entry)
                base_time = datetime.fromisoformat(timestamp[:-1])  # Update base time for next action

            elif action_type == "add_to_cart" and viewed_products:
                # Add to cart action (only for products that have been viewed)
                product_id = random.choice(list(viewed_products))  # Pick a viewed product
                category = products[product_id]
                timestamp = random_timestamp(base_time)

                carted_products.add(product_id)  # Mark this product as added to the cart

                entry = {
                    "user_id": f"u{user_id:06d}",
                    "session_id": session_id,  # Include the session ID
                    "campaign_code": campaign_code,  # Include the campaign code (if any)
                    "action_type": "add_to_cart",
                    "product_id": product_id,
                    "category": category,
                    "timestamp": timestamp
                }

                data.append(entry)
                base_time = datetime.fromisoformat(timestamp[:-1])  # Update base time for next action

            elif action_type == "purchase":
                # Purchase action (either direct or after adding to cart)
                if carted_products:
                    # Purchase a product from the cart
                    product_id = random.choice(list(carted_products))
                else:
                    # Direct purchase from viewed products
                    product_id = random.choice(list(viewed_products))

                category = products[product_id]
                timestamp = random_timestamp(base_time, offset_minutes=5)  # Purchase happens after add to cart or view
                quantity = random.randint(1, 5)  # Random quantity (1 to 5 items)
                price_per_item = generate_price()  # Generate random price per item
                total_price = round(quantity * price_per_item, 2)  # Total price for the purchase
                payment_method_id = random.choice(payment_methods)  # Randomly assign a payment method

                entry = {
                    "user_id": f"u{user_id:06d}",
                    "session_id": session_id,  # Include the session ID
                    "campaign_code": campaign_code,  # Include the campaign code (if any)
                    "action_type": "purchase",
                    "product_id": product_id,
                    "category": category,
                    "quantity": quantity,  # Include quantity
                    "total_price": total_price,  # Include total price
                    "payment_method_id": payment_method_id,  # Include payment method ID
                    "timestamp": timestamp
                }

                data.append(entry)
                base_time = datetime.fromisoformat(timestamp[:-1])  # Update base time for next action
            time.sleep(2)
            # yield f"data: {json.dumps(entry)}\n\n"  #For Dashbord
            yield f"{json.dumps(entry)}\n\n"  # Streaming the JSON data to the client


@app.route('/')
def index():
    # HTML page to listen to real-time events
    return render_template('index.html')


@app.route('/events')
def events():
    # Return an event stream for real-time data
    return Response(data_generator(), content_type='text/event-stream')


if __name__ == '__main__':
    app.run(debug=True, threaded=True)
