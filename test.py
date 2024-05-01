from helpers.helper import convert_timestamp_est, datetime_to_timestamp_UTC





# def get_order_info(base_url: str, account_id: str, access_token:str, order_id: str):
    
#     response = requests.post(f'{base_url}accounts/{account_id}/orders/{order_id}', 
#         params={"account_id": account_id, "id": order_id, "includeTags": True}, 
#         headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})
#     print(response.status_code)

#     if response.status_code == 200:
#         response_json = response.json()
#         exec_quantity = response_json['order']['exec_quantity']
#         average_fill_price = response_json['order']['avg_fill_price']
#         last_fill_price = response_json['order']['last_fill_price']
#         transaction_dt = response_json['order']['transaction_date']
#         created_dt = response_json['order']['create_date']
#         return {"average_fill_price": average_fill_price, "last_fill_price": last_fill_price, "exec_quantity": exec_quantity, "transaction_date": transaction_dt, "created_date": created_dt, "status": "Success"}
#     else:
#         print(f"Order information for order_id: {order_id} has failed. Review option contract availability and code.")
#         return {"status": "Does not exist."}
    

# def get_open_trades_by_orderid(order_id_list):
#     response = client.batch_get_item(
#         RequestItems={
#             'icarus-orders-table': {
#                 'Keys': [{'order_id': {'S': id}} for id in order_id_list]
#             }
#         }
#     )
#     items = response['Responses']['icarus-orders-table']
#     print(items)
#     result_df = pd.DataFrame(items)

#     result_df = result_df.applymap(extract_values_from_dict)
#     return result_df

if __name__ == "__main__":
    # x = convert_datestring_to_timestamp_UTC("2024-03-28T15:09:38.959Z")
    x = convert_timestamp_est((1713283505))
    print(x)
    print(type(x))

# # Define a function to extract values from dict objects
# def extract_values_from_dict(d):
#     return list(d.values())[0] if isinstance(d, dict) else d

# full_transaction_data = {
# 1234: {"x":"val","orders":[{11:[112]},{12:[113]}]},
# 112345: {"x":"val","orders":[{11:[112]}]}
# }
# # for position in dictt:
# #     orders = dictt[position]
# #     print(orders)
# #     for order in orders:
# #         print(order,"order")
# #         # transactions = order[order]
# #         for transaction in order:
# #             print("Key:", tra)
# #             print(transaction)

# for position_id in full_transaction_data:
#     position = full_transaction_data[position_id]
#     print(position)
#     for order in position['orders']:
#         for order_id in order:
#             print(order_id)
#             transactions = order[order_id]
#             for transaction in transactions:
#                 print(transaction)

# def break_array_into_partitions(arr):
#     """
#     Breaks an array into equal partitions of less than 100 items each.

#     Args:
#         arr (list): The input array.

#     Returns:
#         list: A list of partitions, where each partition is a list of items.
#     """
#     if len(arr) <= 100:
#         # If the array has 100 or fewer items, return it as-is
#         return [arr]
#     else:
#         # Calculate the number of partitions needed
#         num_partitions = (len(arr) + 99) // 100  # Round up to the nearest integer

#         # Calculate the size of each partition
#         partition_size = len(arr) // num_partitions

#         # Initialize the starting index and ending index for each partition
#         start = 0
#         end = partition_size

#         # Create the partitions
#         partitions = []
#         for i in range(num_partitions):
#             # If it's the last partition, include any remaining items
#             if i == num_partitions - 1:
#                 partitions.append(arr[start:])
#             else:
#                 partitions.append(arr[start:end])

#             # Update the starting and ending index for the next partition
#             start = end
#             end += partition_size

#         return partitions

    

# if __name__ == "__main__":
#     # Example input array with 150 items
#     my_array = list(range(1, 151))

#     # Call the function to break the array into partitions
#     partitions = break_array_into_partitions(my_array)

#     # Print the partitions
#     for i, partition in enumerate(partitions):
#         print(f"Partition {i + 1}: {partition}")
