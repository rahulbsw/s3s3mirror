package org.cobbzilla.s3s3util;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
public class S3ToS3Context {

    @Getter @Setter private S3ToS3Options options;
    @Getter private final S3ToS3Stats stats = new S3ToS3Stats();

}
