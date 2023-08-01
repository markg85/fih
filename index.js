const fastify = require('fastify')({ logger: false })
const axios = require('axios');
const fs = require('fs/promises');
const path = require('path');
const blake3 = require('@noble/hashes/blake3').blake3;
const sharp = require('sharp');

const PORT = process.env.PORT || 9090;
const IMAGEFOLDER = `${require.main.path}/images`
const METADATAFOLDER = `${require.main.path}/metadata`

const imageExistsByHash = async (hash) => {
    try {
        await fs.access(path.join(IMAGEFOLDER, hash))
        return true;
    } catch (error) {
        return false;
    }
}

const metadataFile = async (hash) => {
    const exists = async (hash) => {
        try {
            await fs.access(path.join(METADATAFOLDER, `${hash}.json`))
            return true;
        } catch (error) {
            return false;
        }
    }

    try {
        await exists(METADATAFOLDER, hash)
        const data = await fs.readFile(path.join(METADATAFOLDER, `${hash}.json`))
        let jsonData = JSON.parse(data)
        if (typeof jsonData == 'object') {
            return jsonData;
        }
    } catch (error) {
        console.log(error)
        return {};
    }
    return {}
}

const saveMetadata = async (metadata, hash) => {
    return fs.writeFile(path.join(METADATAFOLDER, `${hash}.json`), JSON.stringify(metadata))
}

const downloadFile = async (url, hash) => {
    let file = null;
    try {
        console.log(`Starting file download with url ${url} and hash ${hash}`)
        const response = await axios({
            method: 'GET',
            url: url,
            responseType: 'stream'
          })

        file = await fs.open(path.join(IMAGEFOLDER, hash), 'w')
        const writer = file.createWriteStream()
        response.data.pipe(writer)

        await new Promise((resolve, reject) => {
            writer.on('finish', async () => {
                console.log(`Completed download with url ${url} and hash ${hash}`)
                resolve()
            })
            writer.on('error', reject)
          })
    } catch (error) {
        throw new Error(error);
    } finally {
        file?.close()
    }
}

const addVariant = (metadata, imageMetadataObject, hash, extension, filename) => {
    const newVariant = {
        cid: 'TODO: CID',
        hash: hash,
        extension: extension,
        filename: filename,
        width: imageMetadataObject.width,
        height: imageMetadataObject.height
    }

    metadata.variants.push(newVariant)
    return newVariant
}

const flattenedVariants = (metadata) => {
    return [metadata.source, ...metadata.variants];
}

const variantFromTallestWithoutGrace = (tallestRequest, extension, variants) => {
    const result = variants.filter(variant => Math.max(variant.width, variant.height) == tallestRequest && variant.extension == extension);
    if (result == []) {
        return false;
    }
    return result[0];
}

const getExistingVariant = (metadata, options) => {
    const variants = flattenedVariants(metadata)
    // console.log(variants)
    // console.log(options)

    if (options?.tallestSide) {
        if (options?.gracePercentage) {
            return variantFromTallestWithoutGrace(options.tallestSide, options.extension, variants)
        } else {
            return variantFromTallestWithoutGrace(options.tallestSide, options.extension, variants)
        }
    }

    return metadata.source;
}

fastify.post('/*', async (request, reply) => {

    let resultingHash = '';
    let filename = null
    let options = {}

    try {
        const url = request.params['*']
        const hash = Buffer.from(blake3(url)).toString('hex')
    
        /**
         * A body like this json can be provided:
         * {
         *    IMPLEMENTED - tallestSide: <number>                         // Defines that the requestor wants an image with the talles side this many pixels.
         *    TODO        - gracePercentage: <number 0-100>               // The requestor wants an image of X pixels (one of the pixel options) but doesn't care if the image is a bit bigger or smaller.
         *    TODO        - width: <number>                               // Without gracePercentage: returns an image with the exact width as requested. With grace, finds the image closest to it or creates it if it doesn't exist yet.
         *    TODO        - height: <number>                              // Same as with only for the height.
         *                                                                // If both width and height are provided then the resulting image needs to fit in that size. Aspect ratio will always be mantained!
         *    IMPLEMENTED - ruturnImage: bool                             // False (default): True returns the (generated) image as response, false (default) return the hash of the (generated) image.
         *    TODO        - returnCID bool                                // True: returns an IPFS CID representing the resulting image. False (default) no CID is returned. All images are put on IPFS too though regardless of this option.
         * 
         *    TODO        - token <access token>                          // The token allowing you access to request images through this method and thereby downloading them on the server too.
         * }
         */
    
        console.log(request.body)
    
        let metadata = null
        let sourceImageSharp = null
    
        // Handle the source file
        try {
            metadata = await metadataFile(hash);
            
            if (await imageExistsByHash(hash) == false) {
                await downloadFile(url, hash)
                sourceImageSharp = sharp(path.join(IMAGEFOLDER, hash))
                const sourceMetadata = await sourceImageSharp.metadata()
                metadata['source'] = {
                    cid: 'TODO: CID',
                    hash: hash,
                    width: sourceMetadata.width,
                    height: sourceMetadata.height
                }
                metadata['variants'] = []
            }
        } catch (error) {
            console.log(`The requested URL could not be parsed as image. URL: ${url}`)
            await fs.rm(path.join(IMAGEFOLDER, hash))
            return reply.status(400).send({ status: "The requested URL could not be parsed as image." })
        }
    
        // Handle the requested options.
        options = request.body;

        if (!options?.extension) {
            options.extension = 'avif'
        }

        if (['heif', 'avif'].includes(options.extension) == false) {
            options.extension = 'avif'
        }
        
        const existingVariant = getExistingVariant(metadata, options)
        if (existingVariant) {
            resultingHash = existingVariant.hash
            filename = existingVariant.filename
            console.log(`Requested image matched existing images. Returned hash: ${resultingHash} (width: ${existingVariant.width}, height: ${existingVariant.height})`)
        } else {
            if (options?.tallestSide) {
                if (sourceImageSharp == null) {
                    sourceImageSharp = sharp(path.join(IMAGEFOLDER, hash));
                }
        
                const sourceImageMetadata = await sourceImageSharp.metadata();
                let resizeOptions = {}
                if (sourceImageMetadata.width > sourceImageMetadata.height) {
                    resizeOptions.width = options.tallestSide;
                } else {
                    resizeOptions.height = options.tallestSide;
                }
        
                const newImageBuffer = await sourceImageSharp.resize(resizeOptions).toBuffer()
                const resizedHash = Buffer.from(blake3(newImageBuffer)).toString('hex')
        
                try {
                    const destImage = sharp(newImageBuffer);
                    filename = `${resizedHash}.${options.extension}`

                    if (options.extension == 'avif') {
                        await destImage.avif({ quality: 75 }).toFile(path.join(IMAGEFOLDER, filename))
                    } else if (options.extension == 'heif') {
                        await destImage.heif({ quality: 75, compression: 'hevc' }).toFile(path.join(IMAGEFOLDER, filename))
                    }

                    resultingHash = resizedHash
                    const newVariant = addVariant(metadata, await destImage.metadata(), resizedHash, options.extension, filename)

                    console.log(`Created new variant image from hash ${hash}. New width: ${newVariant.width} and height: ${newVariant.height}`)
    
                } catch (error) {
                    console.log(error)
                }
            }

            await saveMetadata(metadata, hash);
        }
    
    } catch (error) {
        console.log(error)
    }

    if (options?.ruturnImage == true) {
        const file = await fs.open(path.join(IMAGEFOLDER, resultingHash), 'r')
        const stream = file.createReadStream()
        reply.header('Content-Type', 'image/avif')
        return reply.send(stream)
    } else {
        return {hash: resultingHash, filename: filename}
    }
})

const main = async () => {
    try {

        for (const folder of [IMAGEFOLDER, METADATAFOLDER]) {
            try {
                await fs.access(folder)
            } catch (error) {
                await fs.mkdir(folder);
                console.log(`Folder ${folder} Created Successfully.`);
            }
    
            await fs.access(folder, fs.constants.R_OK | fs.constants.W_OK);
            console.log(`Writing works for: ${folder}`);
        }

        fastify.listen({port: PORT, host: '0.0.0.0'})
    } catch (err) {
        console.log(err)
        process.exit(1)
    }
}

main();