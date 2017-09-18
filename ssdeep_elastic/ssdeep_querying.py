import elasticsearch
import ssdeep


def insert_record_to_ssdeep_index(ssdeep_value, sha256):
    """
    Adds a record to the ssdeep index in elasticsearch
    :param ssdeep_value: The ssdeep hash value of the item
    :param sha256: The sha256 hash value of the item
    """
    chunksize, chunk, double_chunk = ssdeep_value.split(':')
    chunksize = int(chunksize)

    es = elasticsearch.Elasticsearch(['localhost:9200'])

    document = {'chunksize': chunksize, 'chunk': chunk, 'double_chunk': double_chunk, 'ssdeep': ssdeep_value,
                'sha256': sha256}

    es.index('ssdeep-index', 'record', document)
    es.indices.refresh('ssdeep-index')


def get_matching_items_by_ssdeep(ssdeep_value, threshold_grade):
    """
    A function that finds matching items by ssdeep comparison with optimizations using ElasticSearch
    :param ssdeep_value: The ssdeep hash value of the item
    :param threshold_grade: The grade being used as a threshold, only items that pass this grade will be returned
    :return: A List of matching items (in this case, a list of sha256 hash values)
    """
    chunksize, chunk, double_chunk = ssdeep_value.split(':')
    chunksize = int(chunksize)

    es = elasticsearch.Elasticsearch(['localhost:9200'])

    query = {
        'query': {
            'bool': {
                'must': [
                    {
                        'terms': {
                            'chunksize': [chunksize, chunksize * 2, int(chunksize / 2)]
                        }
                    }
                ],
                'should': [
                    {
                        'match': {
                            'chunk': {
                                'query': chunk
                            }
                        }
                    },
                    {
                        'match': {
                            'double_chunk': {
                                'query': double_chunk
                            }
                        }
                    }
                ]
            }
        }
    }

    results = es.search('ssdeep-index', body=query)

    sha256_list_to_return = []

    for record in results['hits']['hits']:
        record_ssdeep = record['_source']['ssdeep']
        ssdeep_grade = ssdeep.compare(record_ssdeep, ssdeep_value)

        if ssdeep_grade >= threshold_grade:
            sha256_list_to_return.append(record['_source']['sha256'])

    return sha256_list_to_return


if __name__ == '__main__':
    item_1 = {'ssdeep': '768:v7XINhXznVJ8CC1rBXdo0zekXUd3CdPJxB7mNmDZkUKMKZQbFTiKKAZTy:ShT8C+fuioHq1KEFoAU',
              'sha256': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'}
    item_2 = {'ssdeep': '768:C7XINhXznVJ8CC1rBXdo0zekXUd3CdPJxB7mNmDZkUKMKZQbFTiKKAZTV6:ThT8C+fuioHq1KEFoAj6',
              'sha256': 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'}
    item_3 = {
        'ssdeep': '768:t2m3D9SlK1TVYatO/tkqzWQDG/ssC7XkZDzYYFTdqiP1msdT1OhN7UmSaED7Etnc:w7atyfzWgGEXszYYF4iosdTE1zz2+Ze',
        'sha256': 'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'}

    insert_record_to_ssdeep_index(item_1['ssdeep'], item_1['sha256'])
    insert_record_to_ssdeep_index(item_3['ssdeep'], item_3['sha256'])

    matching_items = get_matching_items_by_ssdeep(item_2['ssdeep'], 90)

    print(matching_items)  # This will only print The first item
