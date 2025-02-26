const fastify = require('fastify')({ logger: false })
const axios = require('axios');
const fs = require('fs/promises');
const path = require('path');
const blake3 = require('@noble/hashes/blake3').blake3;
const sharp = require('sharp');

const PORT = process.env.PORT || 9090;
const IMAGEFOLDER = `${require.main.path}/images`
const METADATAFOLDER = `${require.main.path}/metadata`

const pendingDownloads = new Set();

function isEmptyObject(obj) {
    return Object.keys(obj).length === 0;
}

const imageExists = async (hash) => {
    try {
        await fs.access(path.join(IMAGEFOLDER, hash))
        return true;
    } catch (error) {
        return false;
    }
}

// const touch = async (path) => {
//     console.log(path)
//     return new Promise(async (resolve, reject) => {
//         try {
//             await fs.access(path, fs.constants.R_OK | fs.constants.W_OK)
//             resolve()
//             return;
//         } catch (error) {
//             // nothing. Not an "error" error...
//         }
        
//         await fs.writeFile(path, "{}")
//         resolve()
//         return;
//     });
//   };

// const metadataFile = async (hash) => {
//     try {
//         metadatafilerwlock.add(hash);
//         const filePath = path.join(METADATAFOLDER, `${hash}.json`)
//         await touch(filePath);
//         const data = await fs.readFile(filePath)
//         let jsonData = JSON.parse(data.toString())
//         if (typeof jsonData == 'object') {
//             return jsonData;
//         }
//     } catch (error) {
//         // console.log(error)
//         throw new Error(`Killed metadataFile function, someone else is still reading/writing.`);
//     } finally {
//         return {}
//     }
// }

// const saveMetadata = async (metadata) => {
//     return fs.writeFile(path.join(METADATAFOLDER, `${metadata.source.hash}.json`), JSON.stringify(metadata))
// }

const downloadFile = async (url, hash) => {
    let file = null;
    try {
        pendingDownloads.add(hash);
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
        pendingDownloads.delete(hash);
    }
}

// const addVariant = (metadata, imageMetadataObject, hash, extension, filename) => {
//     const newVariant = {
//         cid: 'TODO: CID',
//         hash: hash,
//         extension: extension,
//         filename: filename,
//         width: imageMetadataObject.width,
//         height: imageMetadataObject.height
//     }

//     metadata.variants.push(newVariant)
//     return newVariant
// }

// const flattenedVariants = (metadata) => {
//     return [metadata.source, ...metadata.variants];
// }

// const variantFromTallestWithoutGrace = (tallestRequest, extension, variants) => {
//     const result = variants.filter(variant => Math.max(variant.width, variant.height) == tallestRequest && variant.extension == extension);
//     if (result == []) {
//         return false;
//     }
//     return result[0];
// }

// const getExistingVariant = (metadata, options) => {
//     const variants = flattenedVariants(metadata)
//     // console.log(variants)
//     // console.log(options)

//     if (options?.tallestSide) {
//         if (options?.gracePercentage) {
//             return variantFromTallestWithoutGrace(options.tallestSide, options.extension, variants)
//         } else {
//             return variantFromTallestWithoutGrace(options.tallestSide, options.extension, variants)
//         }
//     }

//     return metadata.source;
// }

// const handleProcessing = async (metadata, options, sourceImageSharp) => {
//     const existingVariant = getExistingVariant(metadata, options)
    
//     if (existingVariant) {
//         console.log(`Requested image matched existing images. Returned hash: ${existingVariant.hash} (width: ${existingVariant.width}, height: ${existingVariant.height})`)
//         return {hash: existingVariant.hash, filename: existingVariant.filename}
//     } else {
//         const hash = metadata.source.hash;

//         if (options?.tallestSide) {
//             if (sourceImageSharp == null) {
//                 sourceImageSharp = sharp(path.join(IMAGEFOLDER, hash));
//             }

//             const sourceImageMetadata = await sourceImageSharp.metadata();
//             let resizeOptions = {}
//             if (sourceImageMetadata.width > sourceImageMetadata.height) {
//                 resizeOptions.width = options.tallestSide;
//             } else {
//                 resizeOptions.height = options.tallestSide;
//             }
    
//             const newImageBuffer = await sourceImageSharp.resize(resizeOptions).toBuffer()
//             const resizedHash = Buffer.from(blake3(newImageBuffer)).toString('hex')
    
//             try {
//                 const destImage = sharp(newImageBuffer);
//                 filename = `${resizedHash}.${options.extension}`

//                 if (options.extension == 'avif') {
//                     await destImage.avif({ quality: 75 }).toFile(path.join(IMAGEFOLDER, filename))
//                 } else if (options.extension == 'heif') {
//                     await destImage.heif({ quality: 75, compression: 'hevc' }).toFile(path.join(IMAGEFOLDER, filename))
//                 }

//                 const newVariant = addVariant(metadata, await destImage.metadata(), resizedHash, options.extension, filename)
//                 await saveMetadata(metadata);
//                 console.log(`Created new variant image from hash ${hash}. New width: ${newVariant.width} and height: ${newVariant.height}`)

//                 return {hash: newVariant.hash, filename: newVariant.filename}
//             } catch (error) {
//                 console.log(error)
//                 return null;
//             }
//         }
//         return null;
//     }
// }

fastify.post('/*', async (request, reply) => {

    try {
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

        // // Handle the requested options.
        let options = request.body;

        if (!options?.extension) {
            options.extension = 'avif'
        }

        if (['jxl', 'avif'].includes(options.extension) == false) {
            options.extension = 'avif'
        }

        const urlHash = Buffer.from(blake3(request.params['*'])).toString('hex')

        if (pendingDownloads.has(urlHash)) {
            return reply.status(408).send({ status: "The download is in progress!" })
        } else {
            if (!await imageExists(urlHash)) {
                pendingDownloads.add(urlHash)
                await downloadFile(request.params['*'], urlHash);
            }

            const sourceImageSharp = sharp(path.join(IMAGEFOLDER, urlHash))
            let resizeOptions = {}

            if (options?.tallestSide) {
                const sourceMetadata = await sourceImageSharp.metadata();
                if (sourceMetadata.width > sourceMetadata.height) {
                    resizeOptions.width = options.tallestSide;
                } else {
                    resizeOptions.height = options.tallestSide;
                }
            }

            // The destination file we're looking for is...
            let destinationHash = `${Buffer.from(blake3(urlHash + JSON.stringify(request.body))).toString('hex')}`
            let destinationFile = `${destinationHash}.${options.extension}`

            if (await imageExists(destinationFile) == false) {
                await sourceImageSharp.resize(resizeOptions).avif({ quality: 75 }).toFile(path.join(IMAGEFOLDER, destinationFile))
                console.log(`created requested image with target filename: ${destinationFile}`)
            } else {
                console.log(`destination exists. Returned with filename: ${destinationFile}`)
            }

            return {hash: destinationHash, filename: destinationFile}
        }
    } catch (error) {
        console.log(error)
        console.log(`The requested URL could not be parsed as image. URL: ${url}`)
        await fs.rm(path.join(IMAGEFOLDER, hash))
        return reply.status(400).send({ status: "The requested URL could not be parsed as image." })
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
