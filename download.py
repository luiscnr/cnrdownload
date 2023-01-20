
def main():
    print('STARTED')
    from eumdac_lois import EUMDAC_LOIS
    edac = EUMDAC_LOIS(True)
    outputdir = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_TEST'
    products ,product_names ,collection_id = edac.search_olci_by_point('2023-01-19' ,'FR' ,'L2' ,45.324091, 12.527398
                                                                       ,-1 ,-1)
    edac.download_product_from_product_list(products ,outputdir)

if __name__ == '__main__':
    main()
