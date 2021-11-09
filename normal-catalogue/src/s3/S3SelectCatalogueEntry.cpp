//
// Created by matt on 27/3/20.
//

#include <normal/connector/s3/S3SelectCatalogueEntry.h>

normal::connector::s3::S3SelectCatalogueEntry::S3SelectCatalogueEntry(const std::string& Alias,
																	  std::shared_ptr<S3SelectPartitioningScheme> partitioningScheme,
                                                                           std::shared_ptr<normal::connector::Catalogue> catalogue)
    : normal::connector::CatalogueEntry(Alias, std::move(catalogue)), partitioningScheme_(std::move(partitioningScheme)) {}
