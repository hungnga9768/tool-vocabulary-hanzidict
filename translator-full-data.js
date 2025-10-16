import fs from 'fs';
import puppeteer from 'puppeteer';
import * as cheerio from 'cheerio';
import Papa from 'papaparse';
import pLimit from 'p-limit';

// Configuration
const INPUT_FILE = 'vocabulary.csv';
const OUTPUT_FILE = 'translated-full.csv';
const CONCURRENT_REQUESTS = 2;
const REQUEST_DELAY = 3000;
const MAX_RETRIES = 2;

// Rate limiter
const limit = pLimit(CONCURRENT_REQUESTS);

// Statistics
let stats = {
    total: 0,
    successful: 0,
    failed: 0,
    skipped: 0,
    startTime: Date.now()
};

// Global browser instance
let browser = null;

/**
 * Sleep function for delays
 */
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Initialize browser
 */
async function initBrowser() {
    if (!browser) {
        console.log('🚀 Starting browser...');
        browser = await puppeteer.launch({
            headless: 'new',
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--disable-gpu'
            ]
        });
        console.log('✅ Browser started');
    }
    return browser;
}

/**
 * Close browser
 */
async function closeBrowser() {
    if (browser) {
        console.log('🔒 Closing browser...');
        await browser.close();
        browser = null;
    }
}

/**
 * Extract all data from Hanzii.net page
 */
async function extractFullData(chineseWord, retryCount = 0) {
    let page = null;
    
    try {
        const browserInstance = await initBrowser();
        page = await browserInstance.newPage();
        
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
        await page.setViewport({ width: 1366, height: 768 });
        
        const url = `https://hanzii.net/search/word/${encodeURIComponent(chineseWord)}?hl=vi`;
        console.log(`Fetching: ${chineseWord} (${url})`);
        
        await page.goto(url, { 
            waitUntil: 'networkidle0',
            timeout: 30000 
        });
        
        try {
            await page.waitForSelector('.txt-mean, .box-mean, .simple-tradition-wrap', { 
                timeout: 15000 
            });
        } catch (waitError) {
            console.log(`⏰ Timeout waiting for content to load for ${chineseWord}`);
        }
        
        await sleep(2000);
        
        const content = await page.content();
        const $ = cheerio.load(content);
        
        // Initialize result object with all fields
        const result = {
            simplified_chinese: chineseWord,
            traditional_chinese: '',
            pinyin_latin: '',
            pinyin_zhuyin: '',
            pinyin_vietnamese: '',
            level: '',
            vietnamese_meaning: '',
            chinese_explanation: '',
            example_sentence_chinese: '',
            example_sentence_pinyin: '',
            grammar_pattern: '',
            related_compounds: '',
            radical_info: '',
            stroke_count: '',
            stroke_order: '',
            popularity: ''
        };
        
        // Extract Traditional Chinese (if different from simplified)
        $('.simple-tradition-wrap').each((i, el) => {
            const text = $(el).text().trim();
            if (text && text !== chineseWord && text.match(/[\u4e00-\u9fff]/)) {
                if (!result.traditional_chinese && text.length <= chineseWord.length + 2) {
                    result.traditional_chinese = text;
                }
            }
        });
        
        // Extract Pinyin variations
        $('.txt-pinyin').each((i, el) => {
            const text = $(el).text().trim();
            if (text && text.includes('[') && text.includes(']')) {
                const cleanText = text.replace(/[\[\]]/g, '').trim();
                
                // Latin pinyin (with tones)
                if (cleanText.match(/[a-zA-Z]/)) {
                    if (!result.pinyin_latin) {
                        result.pinyin_latin = cleanText;
                    }
                }
                
                // Zhuyin (Bopomofo)
                if (cleanText.match(/[ㄅ-ㄩ]/)) {
                    result.pinyin_zhuyin = cleanText;
                }
            }
        });
        
        // Extract Vietnamese pronunciation
        $('.txt-cn_vi').each((i, el) => {
            const text = $(el).text().trim();
            if (text && text.includes('[') && text.includes(']')) {
                result.pinyin_vietnamese = text.replace(/[\[\]]/g, '').trim();
            }
        });
        
        // Extract level
        $('.txt-slot').each((i, el) => {
            const text = $(el).text().trim();
            if (text && text.match(/^\d+$/)) {
                result.level = text;
            }
        });
        
        // Extract Vietnamese meaning (primary)
        let meaning = '';
        
        // Method 1: Main meaning
        const meaningElement = $('.txt-mean .simple-tradition-wrap').first();
        if (meaningElement.length > 0) {
            meaning = meaningElement.text().trim();
        }
        
        // Method 2: Box meaning
        if (!meaning) {
            const boxMeanElement = $('.box-mean .txt-mean').first();
            if (boxMeanElement.length > 0) {
                meaning = boxMeanElement.text().trim().replace(/^\d+\.\s*/, '');
            }
        }
        
        // Method 3: Look for Vietnamese text
        if (!meaning) {
            $('.simple-tradition-wrap').each((i, el) => {
                const text = $(el).text().trim();
                if (text && text !== chineseWord && 
                    text.match(/[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]/)) {
                    if (!meaning) {
                        meaning = text;
                        return false;
                    }
                }
            });
        }
        
        result.vietnamese_meaning = meaning || 'Không tìm thấy';
        
        // Extract Chinese explanation
        $('.txt-mean-explain .simple-tradition-wrap').each((i, el) => {
            const text = $(el).text().trim();
            if (text && text.match(/[\u4e00-\u9fff]/)) {
                result.chinese_explanation = text;
                return false;
            }
        });
        
        // Extract example sentences
        let exampleFound = false;
        $('.simple-tradition-wrap').each((i, el) => {
            const text = $(el).text().trim();
            if (!exampleFound && text && text.includes('。') && text.match(/[\u4e00-\u9fff]/)) {
                result.example_sentence_chinese = text;
                exampleFound = true;
            }
        });
        
        // Extract example sentence pinyin
        $('.ex-phonetic').each((i, el) => {
            const text = $(el).text().trim();
            if (text && text.match(/[a-zA-Z]/)) {
                result.example_sentence_pinyin = text;
                return false;
            }
        });
        
        // Extract grammar pattern
        $('.simple-tradition-wrap').each((i, el) => {
            const text = $(el).text().trim();
            if (text && text.includes('+') && text.includes(chineseWord)) {
                result.grammar_pattern = text;
                return false;
            }
        });
        
        // Extract related compounds
        const compounds = [];
        $('.txt-compound').each((i, el) => {
            const text = $(el).text().trim();
            if (text && text.match(/[\u4e00-\u9fff]/)) {
                const cleanText = text.replace(/^\d+\.\s*/, '');
                if (cleanText && cleanText !== chineseWord) {
                    compounds.push(cleanText);
                }
            }
        });
        result.related_compounds = compounds.slice(0, 5).join('; '); // Limit to 5 compounds
        
        // Extract radical and stroke information
        $('.txt-detail').each((i, el) => {
            const text = $(el).text().trim();
            
            if (text.includes('Bộ:')) {
                result.radical_info = text.replace('Bộ:', '').trim();
            }
            
            if (text.includes('Số nét:')) {
                const match = text.match(/Số nét:\s*(\d+)/);
                if (match) {
                    result.stroke_count = match[1];
                }
            }
            
            if (text.includes('Nét bút:')) {
                result.stroke_order = text.replace('Nét bút:', '').trim();
            }
        });
        
        // Extract popularity info
        $('[class*="txt-detail"]').each((i, el) => {
            const text = $(el).text().trim();
            if (text.includes('Độ phổ biến')) {
                result.popularity = text;
                return false;
            }
        });
        
        // Clean up all fields
        Object.keys(result).forEach(key => {
            if (typeof result[key] === 'string') {
                result[key] = result[key]
                    .replace(/\s+/g, ' ')
                    .replace(/^\d+\.\s*/, '')
                    .replace(/^[•·-]\s*/, '')
                    .trim();
            }
        });
        
        console.log(`✅ Extracted data for ${chineseWord}:`);
        console.log(`   Meaning: ${result.vietnamese_meaning.substring(0, 50)}...`);
        console.log(`   Pinyin: ${result.pinyin_latin}`);
        console.log(`   Level: ${result.level}`);
        
        stats.successful++;
        return result;
        
    } catch (error) {
        console.error(`Error fetching ${chineseWord}:`, error.message);
        
        if (retryCount < MAX_RETRIES) {
            console.log(`Retrying ${chineseWord} (attempt ${retryCount + 1}/${MAX_RETRIES})`);
            await sleep(5000);
            return extractFullData(chineseWord, retryCount + 1);
        }
        
        stats.failed++;
        return {
            simplified_chinese: chineseWord,
            traditional_chinese: '',
            pinyin_latin: '',
            pinyin_zhuyin: '',
            pinyin_vietnamese: '',
            level: '',
            vietnamese_meaning: 'Không tìm thấy',
            chinese_explanation: '',
            example_sentence_chinese: '',
            example_sentence_pinyin: '',
            grammar_pattern: '',
            related_compounds: '',
            radical_info: '',
            stroke_count: '',
            stroke_order: '',
            popularity: ''
        };
        
    } finally {
        if (page) {
            await page.close();
        }
    }
}

/**
 * Process a single Chinese word
 */
async function processWord(chineseWord) {
    return limit(async () => {
        const data = await extractFullData(chineseWord);
        await sleep(1000);
        return data;
    });
}

/**
 * Read and parse CSV file
 */
function readCSV(filename) {
    return new Promise((resolve, reject) => {
        const csvData = fs.readFileSync(filename, 'utf8');
        
        Papa.parse(csvData, {
            header: true,
            skipEmptyLines: true,
            complete: (results) => {
                if (results.errors.length > 0) {
                    console.warn('CSV parsing warnings:', results.errors);
                }
                resolve(results.data);
            },
            error: (error) => {
                reject(error);
            }
        });
    });
}

/**
 * Write results to CSV file
 */
function writeCSV(data, filename) {
    const csv = Papa.unparse(data, {
        header: true,
        encoding: 'utf8'
    });
    
    fs.writeFileSync(filename, csv, 'utf8');
}

/**
 * Load existing translations to resume
 */
async function loadExistingTranslations() {
    if (fs.existsSync(OUTPUT_FILE)) {
        try {
            console.log(`📂 Found existing ${OUTPUT_FILE}, loading...`);
            const existingData = await readCSV(OUTPUT_FILE);
            
            // Ensure existingData is an array
            if (!Array.isArray(existingData)) {
                console.log(`⚠️ Invalid data format in ${OUTPUT_FILE}, starting fresh`);
                return { existingData: [], translatedWords: new Set() };
            }
            
            const translatedWords = new Set(
                existingData.map(row => row.simplified_chinese).filter(word => word)
            );
            console.log(`✅ Loaded ${translatedWords.size} existing translations`);
            return { existingData, translatedWords };
        } catch (error) {
            console.log(`⚠️ Error reading ${OUTPUT_FILE}: ${error.message}`);
            console.log(`🔄 Starting fresh...`);
            return { existingData: [], translatedWords: new Set() };
        }
    }
    return { existingData: [], translatedWords: new Set() };
}

/**
 * Main function
 */
async function main() {
    try {
        console.log('🚀 Starting Full Data Extraction from Hanzii.net...');
        console.log(`📖 Reading ${INPUT_FILE}...`);
        
        // Read input CSV
        const inputData = await readCSV(INPUT_FILE);
        console.log(`📊 Found ${inputData.length} rows in CSV`);
        
        // Load existing translations
        const { existingData, translatedWords } = await loadExistingTranslations();
        
        // Extract unique Chinese words that haven't been processed yet
        const allChineseWords = [...new Set(
            inputData
                .map(row => row.simplified_chinese)
                .filter(word => word && word.trim().length > 0)
        )];
        
        const chineseWords = allChineseWords.filter(word => !translatedWords.has(word));
        
        stats.total = chineseWords.length;
        stats.skipped = translatedWords.size;
        
        console.log(`🔤 Total unique words: ${allChineseWords.length}`);
        console.log(`⏭️  Already processed: ${stats.skipped}`);
        console.log(`🆕 Remaining to process: ${stats.total}`);
        console.log(`⚡ Using ${CONCURRENT_REQUESTS} concurrent browser tabs`);
        
        if (stats.total === 0) {
            console.log('🎉 All words already processed!');
            return;
        }
        
        // Start with existing data
        const results = [...existingData];
        
        // Process words in batches
        const batchSize = 5; // Smaller batches for full data extraction
        
        for (let i = 0; i < chineseWords.length; i += batchSize) {
            const batch = chineseWords.slice(i, i + batchSize);
            console.log(`\n📦 Processing batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(chineseWords.length/batchSize)} (${batch.length} words)`);
            
            const batchPromises = batch.map(word => processWord(word));
            const batchResults = await Promise.all(batchPromises);
            results.push(...batchResults);
            
            // Save progress after each batch
            console.log(`💾 Saving progress to ${OUTPUT_FILE}...`);
            writeCSV(results, OUTPUT_FILE);
            
            // Progress update
            const processed = Math.min(i + batchSize, chineseWords.length);
            const percentage = ((processed / chineseWords.length) * 100).toFixed(1);
            const totalProcessed = stats.skipped + processed;
            const totalPercentage = ((totalProcessed / allChineseWords.length) * 100).toFixed(1);
            
            console.log(`✅ Batch progress: ${processed}/${chineseWords.length} (${percentage}%)`);
            console.log(`📊 Overall progress: ${totalProcessed}/${allChineseWords.length} (${totalPercentage}%) - Saved to file`);
            
            // Delay between batches
            if (i + batchSize < chineseWords.length) {
                console.log(`⏳ Waiting ${REQUEST_DELAY/1000}s before next batch...`);
                await sleep(REQUEST_DELAY);
            }
        }
        
        // Calculate execution time
        const executionTime = ((Date.now() - stats.startTime) / 1000).toFixed(2);
        
        // Print final statistics
        console.log('\n🎉 Full data extraction completed!');
        console.log('📈 Statistics:');
        console.log(`   • Total unique words: ${allChineseWords.length}`);
        console.log(`   • Already processed: ${stats.skipped}`);
        console.log(`   • Newly processed: ${stats.successful}`);
        console.log(`   • Failed extractions: ${stats.failed}`);
        console.log(`   • Success rate: ${((stats.successful / stats.total) * 100).toFixed(1)}%`);
        console.log(`   • Execution time: ${executionTime} seconds`);
        console.log(`   • Output file: ${OUTPUT_FILE}`);
        
    } catch (error) {
        console.error('❌ Error:', error.message);
        process.exit(1);
    } finally {
        await closeBrowser();
    }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
    console.log('\n⏹️  Process interrupted by user');
    const executionTime = ((Date.now() - stats.startTime) / 1000).toFixed(2);
    console.log(`📊 Partial results: ${stats.successful} successful, ${stats.failed} failed in ${executionTime}s`);
    console.log(`💾 Progress has been saved to ${OUTPUT_FILE}`);
    await closeBrowser();
    process.exit(0);
});

// Run the main function
main().catch(async (error) => {
    console.error(error);
    await closeBrowser();
    process.exit(1);
});
