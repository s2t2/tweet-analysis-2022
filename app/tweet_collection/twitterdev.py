


# first add a table of 80 domains from here: https://developer.twitter.com/en/docs/twitter-api/annotations/overview





# then add a table of entities from here:
# https://raw.githubusercontent.com/twitterdev/twitter-context-annotations/main/files/evergreen-context-entities-20220601.csv

# each entity has one or more domain_id s
#
# domains,entity_id,entity_name
# ..., ..., ...
# "10,35,131",799022225751871488,Donald Trump
# "10,131",884781076484202496,Donald Trump Jr.



from pandas import read_csv

def domains_list(domains_val):
    """
    Params domains_val : int like 10 or string like "10,56,131"
    """
    return [int(domain_id) for domain_id in str(domains_val).split(",")]



if __name__ == "__main__":

    context_entities_url = "https://raw.githubusercontent.com/twitterdev/twitter-context-annotations/6c349b2f3e1a3e7aca54d941225c485698a93c7a/files/evergreen-context-entities-20220601.csv"
    df = read_csv(context_entities_url)

    # domains is a CSV string like "10,56,131"
    # convert to a list:
    df["domain_ids"] = df["domains"].apply(domains_list)

    print(df.head())

    breakpoint()
