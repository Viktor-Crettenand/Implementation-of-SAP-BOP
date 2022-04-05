# %%
import pickle
import pandas as pd
import numpy as np
# %%
with open('bop_test.pkl', 'rb') as f:
    outbound = pickle.load(f)
with open('bop_test_inventory.pkl', 'rb') as f:
    inventory = pickle.load(f)
with open('bop_test_inbound.pkl', 'rb') as f:
    inbound = pickle.load(f)
with open('arrival_expiry.pkl', 'rb') as f:
    arrival_expiry = pickle.load(f)
# class_mapping = dict(zip(outbound.customer.unique(), np.random.choice(['premium', 'comfort', 'base'], size=len(outbound.customer.unique()), p=[0.01, 0.04, 0.95])))
class_mapping = dict(zip(outbound.customer.unique(), np.random.choice(['premium', 'comfort', 'base'], size=len(outbound.customer.unique()), p=[0.1, 0.3, 0.6])))
outbound.loc[:, 'customer_class'] = outbound.customer.map(class_mapping)
outbound = outbound[['creation_date', 'latest_process_date', 'demand', 'customer_class']]
inbound.loc[:, 'cum_expired'] = inbound.qty_expired.cumsum()


# %%
#first
outbound.loc[:, 'fulfillment_date'] = pd.NaT
# want to allocate order lines before latest_process_date
# prioritize customers
# consider only information that is already known i.e. creation_date <= present
# need to keep track of available inventory
# append all creation_date and latest_process_date, take the unique ones and loop through them
# pool the unfulfilled orders that are before "present"
pool = pd.DataFrame()
inventory_list = pd.DataFrame()
# inventory_ = inventory[inventory.date < pd.to_datetime('2020-12-10')].tail(1).apply(lambda x: x.qty + x.qty_expired, axis=1).values[0]
inventory_ = 0
sold = 0
cumulative_scraped_inventory_old = 0
for present in np.sort(outbound.creation_date.append(outbound.latest_process_date).append(inbound.date).unique()):
    # add the records of today
    pool = pool.append(outbound[outbound.creation_date == present])
    # order pool by order of priority: priority of cutsomer class and soonest ship due date
    pool = pool.sort_values(['customer_class', 'latest_process_date'], ascending=[False, True])
    pool.loc[:, 'cumsum_'] = pool.demand.cumsum()
    # print(inventory_, present)
    pool.loc[:, 'fulfill'] = (-pool.cumsum_ <= inventory_) & (pool.latest_process_date <= present)
    if len(pool[pool.fulfill == True])>0:
        inventory_ = inventory_ + pool[pool.fulfill == True].demand.sum()
        sold = sold - pool[pool.fulfill == True].demand.sum()
        # print('inventory: ', inventory_, 'sold: ', sold, present)


    outbound.loc[pool[pool.fulfill == True].index, 'fulfillment_date'] = present
    pool = pool[pool.fulfill == False]
    if len(inbound[inbound.date == present]) > 0:
        # print(sold)
        # print(inventory_)
        inventory_list = inventory_list.append(arrival_expiry[arrival_expiry.arrival_date == present])
        inventory_list = inventory_list[inventory_list.expiration_date > present]
        # print(inventory_list)
        # print('Inbound shipment -------------')
        # print('inventory: ', inventory_)
        # print(present)
        # print('sold: ', sold)
        # print(present)
        # print(inbound[inbound.date == present])
        cum_expired = - inbound[inbound.date == present].cum_expired.values[0]
        # add replenishment inventory
        print('before', inventory_, present)
        inventory_ = inventory_ + inbound[inbound.date == present].qty.values[0]
        
        #---------------------------------------------------------------------

        # remove expired inventory that wasn't already sold
        cumulative_scraped_inventory = cumulative_scraped_inventory_old + max(0, cum_expired - cumulative_scraped_inventory_old - sold)

        #---------------------------------------------------------------------

        # print('cumulative scraped: ', cumulative_scraped_inventory, 'cum_epired: ', cum_expired, 'sold: ', sold)
        # inventory_ = inventory_ + min(sold + inbound[inbound.date == present].qty_expired.values[0], 0)
        scraped_now = cumulative_scraped_inventory - cumulative_scraped_inventory_old
        inventory_ = inventory_ - scraped_now
        # print(min(sold + inbound[inbound.date == present].qty_expired.values[0], 0))
        cumulative_scraped_inventory_old = cumulative_scraped_inventory
        # print(inventory_)
        # print('sold', sold)
        # print('inbound', inbound[inbound.date == present].qty.values[0], 'sold', sold, 'inventory', inventory_, 'scraped', cumulative_scraped_inventory)
        print('after', inventory_, present)

outbound.loc[:, 'OTS'] = outbound.latest_process_date >= outbound.fulfillment_date
outbound.groupby(['customer_class', 'OTS']).demand.count().reset_index().groupby('customer_class').apply(lambda df: 100 * df[df.OTS].demand / df.demand.sum()).reset_index().drop(columns='level_1').rename(columns={'demand': 'OTS'})



# %%
#second
outbound.loc[:, 'fulfillment_date'] = pd.NaT
# want to allocate order lines before latest_process_date
# prioritize customers
# consider only information that is already known i.e. creation_date <= present
# need to keep track of available inventory
# append all creation_date and latest_process_date, take the unique ones and loop through them
# pool the unfulfilled orders that are before "present"
pool = pd.DataFrame()
inventory_list = pd.DataFrame()
inventory_list = inventory_list.append(arrival_expiry[arrival_expiry.arrival_date < pd.to_datetime('2020-01-01')])
inventory_ = 0
sold = 0
for present in np.sort(outbound.creation_date.append(outbound.latest_process_date).append(arrival_expiry.arrival_date).append(arrival_expiry.expiration_date).unique()):
    # add the records of today
    pool = pool.append(outbound[outbound.creation_date == present])
    # order pool by order of priority: priority of cutsomer class and soonest ship due date
    pool = pool.sort_values(['customer_class', 'latest_process_date'], ascending=[False, True])
    pool.loc[:, 'cumsum_'] = pool.demand.cumsum()
    # print(inventory_, present)
    pool.loc[:, 'fulfill'] = (-pool.cumsum_ <= inventory_) & (pool.latest_process_date <= present)
    # print(pool.demand.sum())
    if len(pool[pool.fulfill == True])>0:
        inventory_ = inventory_ + pool[pool.fulfill == True].demand.sum()
        sold = sold - pool[pool.fulfill == True].demand.sum()
        # print('inventory: ', inventory_, 'sold: ', sold, present)

    outbound.loc[pool[pool.fulfill == True].index, 'fulfillment_date'] = present
    pool = pool[pool.fulfill == False]
    if len(arrival_expiry[(arrival_expiry.arrival_date == present) | (arrival_expiry.expiration_date == present)]) > 0:
        # print(sold)
        # remove the sold items from inventory
        inventory_list.sort_values('expiration_date', inplace=True)
        inventory_list.loc[:, 'cumsum_'] = inventory_list.qty.cumsum()
        inventory_list = inventory_list[inventory_list.cumsum_ > sold]
        display(inventory_list)
        if len(inventory_list) > 0:
            # print('before', '\n', inventory_list)
            inventory_list.iloc[0, 1] = inventory_list.iloc[0, 4] - sold
            # print('after','\n', inventory_list)
        sold = 0
        # add replenishment inventory
        inventory_list = inventory_list.append(arrival_expiry[arrival_expiry.arrival_date == present])
        inventory_list = inventory_list[inventory_list.expiration_date > present]
        # print('before', inventory_, present)
        inventory_ = inventory_list.qty.sum()
        # print('after', inventory_, present)
        

outbound.loc[:, 'OTS'] = outbound.latest_process_date >= outbound.fulfillment_date
outbound.groupby(['customer_class', 'OTS']).demand.count().reset_index().groupby('customer_class').apply(lambda df: 100 * df[df.OTS].demand / df.demand.sum()).reset_index().drop(columns='level_1').rename(columns={'demand': 'OTS'})

# %%
