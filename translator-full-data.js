import fs from 'fs';
import puppeteer from 'puppeteer';
import * as cheerio from 'cheerio';
import Papa from 'papaparse';
import pLimit from 'p-limit';

// Configuration
const INPUT_FILE = 'vocabulary.csv';
const OUTPUT_FILE = 'translated-full.csv';
const CONCURRENT_REQUESTS = 3; // Tăng lên 3 để xử lý song song
const REQUEST_DELAY = 2000; // Reduced delay
const MAX_RETRIES = 3; // Tăng số lần retry
const MEMORY_CLEANUP_INTERVAL = 10; // Clean memory every 10 words
const SKIP_AFTER_FAILURES = 5; // Skip từ sau 5 lần thất bại liên tiếp

// Rate limiter
const limit = pLimit(CONCURRENT_REQUESTS);

// Statistics
let stats = {
    total: 0,
    successful: 0,
    failed: 0,
    skipped: 0,
    consecutiveFailures: 0,
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
 * Monitor and log memory usage
 */
function logMemoryUsage(context = '') {
    const memUsage = process.memoryUsage();
    const heapUsedMB = Math.round(memUsage.heapUsed / 1024 / 1024);
    const heapTotalMB = Math.round(memUsage.heapTotal / 1024 / 1024);
    const rssMB = Math.round(memUsage.rss / 1024 / 1024);
    
    console.log(`📊 ${context} Memory: Heap ${heapUsedMB}/${heapTotalMB}MB, RSS ${rssMB}MB`);
    
    // Warning if memory usage is high
    if (heapUsedMB > 1000) {
        console.warn(`⚠️ High memory usage detected: ${heapUsedMB}MB`);
    }
}

/**
 * Initialize browser with memory-efficient settings
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
                '--disable-gpu',
                '--memory-pressure-off',
                '--max_old_space_size=4096',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding',
                '--disable-features=TranslateUI',
                '--disable-ipc-flooding-protection'
            ]
        });
        console.log('✅ Browser started with memory-efficient settings');
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
export async function extractFullData(chineseWord, retryCount = 0) {
    let page = null;
    
    try {
        const browserInstance = await initBrowser();
        page = await browserInstance.newPage();
        
        // Set memory-efficient options
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
        await page.setViewport({ width: 1366, height: 768 });
        
        // Disable images and CSS to save memory
        await page.setRequestInterception(true);
        page.on('request', (req) => {
            if (req.resourceType() === 'stylesheet' || 
                req.resourceType() === 'image' || 
                req.resourceType() === 'font' ||
                req.resourceType() === 'media') {
                req.abort();
            } else {
                req.continue();
            }
        });
        
        const url = `https://hanzii.net/search/word/${encodeURIComponent(chineseWord)}?hl=vi`;
        console.log(`Fetching: ${chineseWord} (${url})`);
        
        await page.goto(url, { 
            waitUntil: 'domcontentloaded',
            timeout: 60000 
        });
        
        try {
            await page.waitForSelector('.txt-mean, .box-mean, .simple-tradition-wrap', { 
                timeout: 15000 
            });
        } catch (waitError) {
            console.log(`⏰ Timeout waiting for content to load for ${chineseWord}`);
        }
        
        await sleep(3000); // Tăng thời gian chờ
        
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
            example_sentence_vietnamese: '',
            grammar_pattern: '',
            related_compounds: '',
            radical_info: '',
            stroke_count: '',
            stroke_order: '',
            popularity: '',
            word_type: '',
            measure_words: '',
            synonyms: '',
            hsk_level: '',
            tocfl_level: '',
            popularity_rank: '',
            search_count: '',
            image_url: '',
            topic_category: '',
            ai_level: ''
        };
        
        // Extract Traditional Chinese from wrap-convert elements
        $('.wrap-convert').each((i, el) => {
            const text = $(el).text().trim();
            // Remove brackets and extract traditional characters
            const traditional = text.replace(/[【】]/g, '').trim();
            if (traditional && traditional !== chineseWord && traditional.match(/[\u4e00-\u9fff]/)) {
                result.traditional_chinese = traditional;
                return false;
            }
        });
        
        // Fallback: look in simple-tradition-wrap
        if (!result.traditional_chinese) {
            $('.simple-tradition-wrap').each((i, el) => {
                const text = $(el).text().trim();
                if (text && text !== chineseWord && text.match(/[\u4e00-\u9fff]/) && text.length <= chineseWord.length + 2) {
                    result.traditional_chinese = text;
                    return false;
                }
            });
        }
        
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
        
        // Extract AI level from txt-slot
        $('.txt-slot').each((i, el) => {
            const text = $(el).text().trim();
            if (text && text.match(/^\d+$/)) {
                result.ai_level = text;
            }
        });
        
        // Extract HSK and TOCFL levels
        $('.tags').each((i, el) => {
            const text = $(el).text().trim();
            if (text.includes('HSK')) {
                result.hsk_level = text;
            } else if (text.includes('TOCFL')) {
                result.tocfl_level = text;
            }
        });
        
        // Extract word type (Danh từ, Động từ, etc.)
        $('.box-title').each((i, el) => {
            const text = $(el).text().trim();
            if (text && (text.includes('Danh từ') || text.includes('Động từ') || text.includes('Tính từ') || text.includes('Phó từ'))) {
                result.word_type = text;
            }
        });
        
        // Extract Vietnamese meaning (all meanings combined)
        const meanings = [];
        
        // Method 1: Extract all meanings from .box-mean elements (target the simple-tradition-wrap inside)
        $('.box-mean .txt-mean .simple-tradition-wrap').each((i, el) => {
            const text = $(el).text().trim();
            if (text && text.match(/[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]/)) {
                meanings.push(text);
            }
        });
        
        // Method 2: If no meanings found, try main meaning elements
        if (meanings.length === 0) {
            $('.txt-mean .simple-tradition-wrap').each((i, el) => {
                const text = $(el).text().trim();
                if (text && text !== chineseWord && 
                    text.match(/[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]/)) {
                    meanings.push(text);
                }
            });
        }
        
        // Method 3: Look for Vietnamese text in simple-tradition-wrap if still no meanings
        if (meanings.length === 0) {
            $('.simple-tradition-wrap').each((i, el) => {
                const text = $(el).text().trim();
                if (text && text !== chineseWord && 
                    text.match(/[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]/) &&
                    !text.match(/[\u4e00-\u9fff]/) && // Exclude Chinese characters
                    text.length > 3 && text.length < 200) {
                    meanings.push(text);
                }
            });
        }
        
        // Remove duplicates and combine meanings
        const uniqueMeanings = [...new Set(meanings)];
        result.vietnamese_meaning = uniqueMeanings.length > 0 ? uniqueMeanings.join('; ') : 'Không tìm thấy';
        
        // Extract Chinese explanation
        $('.txt-mean-explain .simple-tradition-wrap').each((i, el) => {
            const text = $(el).text().trim();
            if (text && text.match(/[\u4e00-\u9fff]/)) {
                result.chinese_explanation = text;
                return false;
            }
        });
        
        // Extract example sentences with better targeting
        // Look specifically in example sections
        $('example .box-example').first().each((i, el) => {
            const $example = $(el);
            
            // Extract Chinese sentence
            const chineseSentence = $example.find('.simple-tradition-wrap').first().text().trim();
            if (chineseSentence && chineseSentence.match(/[\u4e00-\u9fff]/)) {
                result.example_sentence_chinese = chineseSentence;
            }
            
            // Extract pinyin
            const pinyin = $example.find('.ex-phonetic').text().trim();
            if (pinyin) {
                result.example_sentence_pinyin = pinyin;
            }
            
            // Extract Vietnamese translation (last text element in example)
            $example.find('div').last().each((j, vietEl) => {
                const vietText = $(vietEl).text().trim();
                if (vietText && 
                    vietText.match(/[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]/) &&
                    !vietText.match(/[\u4e00-\u9fff]/) &&
                    vietText !== pinyin) {
                    result.example_sentence_vietnamese = vietText;
                }
            });
        });
        
        // Fallback: if no example found in structured way, try general search
        if (!result.example_sentence_chinese) {
            $('.simple-tradition-wrap').each((i, el) => {
                const text = $(el).text().trim();
                if (text && text.includes('。') && text.match(/[\u4e00-\u9fff]/) && text.length > 3) {
                    result.example_sentence_chinese = text;
                    return false;
                }
            });
        }
        
        // Fallback for pinyin
        if (!result.example_sentence_pinyin) {
            $('.ex-phonetic').each((i, el) => {
                const text = $(el).text().trim();
                if (text && text.match(/[a-zA-Z]/)) {
                    result.example_sentence_pinyin = text;
                    return false;
                }
            });
        }
        
        // Fallback for Vietnamese translation
        if (!result.example_sentence_vietnamese) {
            $('.box-example .font-16.fw-400.cl-pr-sm').each((i, el) => {
                const text = $(el).text().trim();
                if (text && 
                    text.match(/[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]/) &&
                    !text.match(/[\u4e00-\u9fff]/) &&
                    text.length > 8 &&
                    text.length < 150 &&
                    !text.includes('Trang chủ') &&
                    !text.includes('Dịch')) {
                    result.example_sentence_vietnamese = text;
                    return false;
                }
            });
        }
        
        // Extract grammar pattern
        $('.simple-tradition-wrap').each((i, el) => {
            const text = $(el).text().trim();
            if (text && text.includes('+') && text.includes(chineseWord)) {
                result.grammar_pattern = text;
                return false;
            }
        });
        
        // Extract measure words
        $('.word-deco').each((i, el) => {
            const text = $(el).text().trim();
            if (text && text.includes('[') && text.includes(']')) {
                // Extract measure words like "个, 位, 名 [gè, wèi, míng]"
                const measureMatch = text.match(/([^\[]+)\s*\[([^\]]+)\]/);
                if (measureMatch) {
                    result.measure_words = measureMatch[1].trim() + ' [' + measureMatch[2].trim() + ']';
                }
            }
        });
        
        // Extract synonyms from compound section with better parsing
        const synonyms = [];
        $('#syno .compound .txt-compound').each((i, el) => {
            const $el = $(el);
            const chineseText = $el.find('.simple-tradition-wrap').text().trim();
            const vietnameseText = $el.text().replace(chineseText, '').trim();
            
            if (chineseText && chineseText.match(/[\u4e00-\u9fff]/) && chineseText !== chineseWord) {
                // Combine Chinese and Vietnamese if available
                const synonym = vietnameseText ? `${chineseText} (${vietnameseText})` : chineseText;
                synonyms.push(synonym);
            }
        });
        result.synonyms = synonyms.slice(0, 5).join('; ');
        
        // Extract related compounds (different from synonyms)
        const compounds = [];
        $('.txt-compound').each((i, el) => {
            const text = $(el).text().trim();
            if (text && text.match(/[\u4e00-\u9fff]/) && !synonyms.includes(text)) {
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
        
        // Extract popularity rank and search count
        $('.rank').each((i, el) => {
            const text = $(el).text().trim();
            if (text && text.startsWith('#')) {
                result.popularity_rank = text;
            }
        });
        
        $('.popularity-content').each((i, el) => {
            const text = $(el).text().trim();
            if (text && text.includes('Được tra cứu')) {
                const countMatch = text.match(/Được tra cứu\s*(\d+)\s*lần/);
                if (countMatch) {
                    result.search_count = countMatch[1];
                }
            }
        });
        
        // Extract image URL
        $('img[src*="hanzii.net"]').each((i, el) => {
            const src = $(el).attr('src');
            if (src && src.includes('img_word')) {
                result.image_url = src;
                return false;
            }
        });
        
        // Extract topic category from section with image
        $('.section').each((i, el) => {
            const $el = $(el);
            const text = $el.text().trim();
            // Look for sections with Chinese characters and images (topic categories)
            if (text && text.match(/[\u4e00-\u9fff]/) && $el.find('img').length > 0) {
                result.topic_category = text;
                return false;
            }
        });
        
        // Extract additional multiple meanings if available
        const allMeanings = [];
        $('.content-item .box-mean .txt-mean').each((i, el) => {
            const meaningText = $(el).find('.simple-tradition-wrap').text().trim();
            if (meaningText && meaningText.match(/[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]/)) {
                allMeanings.push(meaningText);
            }
        });
        
        // If we found structured meanings, use them instead
        if (allMeanings.length > 0) {
            const uniqueAllMeanings = [...new Set(allMeanings)];
            result.vietnamese_meaning = uniqueAllMeanings.join('; ');
        }
        
        // Extract popularity info (general)
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
        console.log(`   AI Level: ${result.ai_level}`);
        console.log(`   HSK: ${result.hsk_level}, TOCFL: ${result.tocfl_level}`);
        console.log(`   Type: ${result.word_type}`);
        console.log(`   Measure words: ${result.measure_words}`);
        console.log(`   Synonyms: ${result.synonyms}`);
        console.log(`   Example: ${result.example_sentence_chinese}`);
        console.log(`   Vietnamese ex: ${result.example_sentence_vietnamese}`);
        console.log(`   Popularity: ${result.popularity_rank}`);
        console.log(`   Search count: ${result.search_count}`);
        console.log(`   Topic: ${result.topic_category}`);
        console.log(`   Image: ${result.image_url ? 'Yes' : 'No'}`);
        
        stats.successful++;
        stats.consecutiveFailures = 0; // Reset consecutive failures on success
        return result;
        
    } catch (error) {
        console.error(`❌ Error fetching ${chineseWord}:`, error.message);
        
        if (retryCount < MAX_RETRIES) {
            console.log(`🔄 Retrying ${chineseWord} (attempt ${retryCount + 1}/${MAX_RETRIES})`);
            
            // Close current page if exists
            if (page) {
                try {
                    await page.close();
                } catch (closeError) {
                    console.warn(`Warning closing page: ${closeError.message}`);
                }
            }
            
            // Wait longer between retries
            const waitTime = (retryCount + 1) * 10000; // 10s, 20s, 30s...
            console.log(`⏳ Waiting ${waitTime/1000}s before retry...`);
            await sleep(waitTime);
            
            return extractFullData(chineseWord, retryCount + 1);
        }
        
        stats.failed++;
        stats.consecutiveFailures++;
        
        console.warn(`⚠️ Failed to process ${chineseWord} after ${MAX_RETRIES} attempts`);
        console.warn(`📊 Consecutive failures: ${stats.consecutiveFailures}`);
        
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
            example_sentence_vietnamese: '',
            grammar_pattern: '',
            related_compounds: '',
            radical_info: '',
            stroke_count: '',
            stroke_order: '',
            popularity: '',
            word_type: '',
            measure_words: '',
            synonyms: '',
            hsk_level: '',
            tocfl_level: '',
            popularity_rank: '',
            search_count: '',
            image_url: '',
            topic_category: '',
            ai_level: ''
        };
        
    } finally {
        if (page) {
            try {
                // Clear page cache and close properly
                await page.evaluate(() => {
                    // Clear memory
                    if (window.gc) {
                        window.gc();
                    }
                });
                await page.close();
                console.log(`🗑️ Closed page for ${chineseWord}`);
            } catch (closeError) {
                console.warn(`Warning: Error closing page for ${chineseWord}:`, closeError.message);
            }
        }
    }
}

/**
 * Process a single Chinese word with memory management
 */
async function processWord(chineseWord, wordIndex = 0) {
    return limit(async () => {
        const data = await extractFullData(chineseWord);
        
        // Force garbage collection every few words
        if (wordIndex % MEMORY_CLEANUP_INTERVAL === 0 && global.gc) {
            console.log(`🧹 Running garbage collection after ${wordIndex} words`);
            global.gc();
        }
        
        await sleep(2000); // Tăng delay giữa các request
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
        logMemoryUsage('Initial');
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
        
        // Process words in small batches with concurrency
        const batchSize = 5; // Xử lý 5 từ một lúc
        console.log(`🔄 Processing words in batches of ${batchSize} with ${CONCURRENT_REQUESTS} concurrent requests...`);
        
        for (let i = 0; i < chineseWords.length; i += batchSize) {
            const batch = chineseWords.slice(i, i + batchSize);
            const batchNumber = Math.floor(i / batchSize) + 1;
            const totalBatches = Math.ceil(chineseWords.length / batchSize);
            
            console.log(`\n📦 Processing batch ${batchNumber}/${totalBatches} (${batch.length} words): ${batch.join(', ')}`);
            
            // Check if too many consecutive failures
            if (stats.consecutiveFailures >= SKIP_AFTER_FAILURES) {
                console.warn(`🛑 Too many consecutive failures (${stats.consecutiveFailures}). Pausing for 60 seconds...`);
                await sleep(60000);
                
                // Restart browser after long pause
                console.log(`🔄 Restarting browser after pause...`);
                await closeBrowser();
                await sleep(5000);
                stats.consecutiveFailures = 0; // Reset after restart
            }
            
            try {
                // Process batch concurrently
                const batchPromises = batch.map((word, index) => processWord(word, i + index));
                const batchResults = await Promise.all(batchPromises);
                results.push(...batchResults);
                
                // Show memory usage after each batch
                logMemoryUsage(`Batch ${batchNumber}:`);
                
            } catch (error) {
                console.error(`❌ Error processing batch ${batchNumber}:`, error.message);
                
                // Add failed results for the entire batch
                const failedResults = batch.map(word => ({
                    simplified_chinese: word,
                    traditional_chinese: '',
                    pinyin_latin: '',
                    pinyin_zhuyin: '',
                    pinyin_vietnamese: '',
                    level: '',
                    vietnamese_meaning: 'Lỗi xử lý batch',
                    chinese_explanation: '',
                    example_sentence_chinese: '',
                    example_sentence_pinyin: '',
                    example_sentence_vietnamese: '',
                    grammar_pattern: '',
                    related_compounds: '',
                    radical_info: '',
                    stroke_count: '',
                    stroke_order: '',
                    popularity: '',
                    word_type: '',
                    measure_words: '',
                    synonyms: '',
                    hsk_level: '',
                    tocfl_level: '',
                    popularity_rank: '',
                    search_count: '',
                    image_url: '',
                    topic_category: '',
                    ai_level: ''
                }));
                results.push(...failedResults);
            }
            
            // Save progress after each batch
            console.log(`💾 Saving progress to ${OUTPUT_FILE}...`);
            writeCSV(results, OUTPUT_FILE);
            
            const processed = Math.min(i + batchSize, chineseWords.length);
            const percentage = ((processed / chineseWords.length) * 100).toFixed(1);
            const totalProcessed = stats.skipped + processed;
            const totalPercentage = ((totalProcessed / allChineseWords.length) * 100).toFixed(1);
            
            console.log(`✅ Batch progress: ${processed}/${chineseWords.length} (${percentage}%)`);
            console.log(`📊 Overall progress: ${totalProcessed}/${allChineseWords.length} (${totalPercentage}%) - Saved to file`);
            
            // Restart browser every 4 batches (20 words) to prevent memory buildup
            if (batchNumber % 4 === 0 && i + batchSize < chineseWords.length) {
                console.log(`🔄 Restarting browser to free memory after ${batchNumber} batches...`);
                await closeBrowser();
                await sleep(3000); // Wait before restarting
            }
            
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
        logMemoryUsage('Final');
        
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
