<?php
include_once get_stylesheet_directory() . '/json-ld/faq-json-ld.php';

function road_tax_faqs()
{
    // FAQ data
    $faqs = [
        [
            'question' => 'Apa saja 3 macam asuransi mobil?',
            'answer' => 'a. TLO
Menjamin kerugian akibat kehilangan atau kerusakan yang menyebabkan mobil tidak bisa berfungsi atau nilai perbaikan mobil yang mencapai 75% akibat kecelakaan.
b. Comprehensive
Menjamin kerugian yang disebabkan oleh kecelakaan mobil seperti lecet, penyok, sampai dengan kerusakan yang besar.

c. All Risk
Merupakan paket lengkap dari asuransi mobil comprehensive, yang ditambahkan dengan perlindungan seperti: kecelakaan pengemudi/penumpang, banjir, gempa bumi, huru-hara, terorisme, serta tanggung jawab pihak ke-3.'
        ],
        [
            'question' => 'Bagaimana cara kerja asuransi mobil di Indonesia?',
            'answer' => 'Cara kerja asuransi mobil bekerja antara lain pada saat nasabah membayar premi kepada sebuah perusahaan asuransi, premi tersebut bersama dengan premi pemegang polis yang lain akan masuk ke dalam penghimpunan dana besar yang dibentuk oleh pihak asuransi.
Kemudian perusahaan asuransi akan mengelola dana ini, dan uang dari dana tersebut akan digunakan untuk memberi kompensasi pemegang polis yang sedang mengalami musibah kecelakaan.'
        ],
        [
            'question' => 'Apakah Asuransi Mobil Wajib di Indonesia?',
            'answer' => 'Pentingnya memiliki asuransi kendaraan adalah bisa memberikan perlindungan apabila terjadi risiko. Walaupun sebagian masyarakat meyakini risiko dapat dihindari dengan mengemudi secara hati-hati. Akan tetapi, angka kecelakaan di Jakarta khususnya masih sangat tinggi dengan rata-rata 27 kasus kecelakaan lalu lintas per harinya.

Banyak orang yang mengabaikan potensi biaya kerusakan yang dapat dibebankan pada mereka dan mengabaikan peran dan cara kerja asuransi mobil dalam melindungi mereka terhadap kejadian yang tidak menguntungkan ini.

Memiliki jaminan asuransi mobil dapat membantu meringankan beban finansial. Karena kecelakaan atau kehilangan kendaraan kamu. Lebih penting lagi, asuransi ini dapat mencegah seseorang menjadi korban pengemudi yang membahayakan.'
        ],
        [
            'question' => 'Apakah saya membutuhkan asuransi saat membeli mobil bekas?',
            'answer' => 'Asuransi untuk mobil bekas tentu bisa dibeli. Namun, perlindungan yang bisa dipilih tentu berbeda dengan asuransi untuk kendaraan mobil baru. Pada mobil baru, kita bisa memilih antara asuransi all-risk (komprehensif) atau asuransi TLO. Sedangkan mobil bekas biasanya hanya asuransi TLO saja. Namun ini tergantung merek dan usia mobil serta kebijakan penyedia asuransi.'
        ],
        [
            'question' => 'Asuransi mobil yang mana paling bagus di Indonesia',
            'answer' => 'a. Asuransi Jasindo
Salah satu pilihan asuransi mobil murah berasal dari perusahaan asuransi Jasindo. Penyedia asuransi mobil murah ini memberikan berbagai pilihan premi. Mulai dari 1,05% untuk harga mobil di atas 800 juta, dan mulai dari 3,82% untuk mobil yang bernilai dibawah 125 juta rupiah. Asuransi ini memiliki tipe asuransi all risk.

b. Garda Oto
Garda Oto menawarkan dua jenis asuransi, yakni TPO dan all risk. Perusahaan asuransi ini pun menjadi salah satu yang tepercaya karena telah berdiri sejak 2013. Nilai premi yang dikenakan senilai 2,12% dari harga mobil.

c. Asuransi ACA
Sejumlah 3% merupakan angka yang ditawarkan asuransi ACA untuk premi asuransi all risk yang mereka miliki. Angka ini berlaku untuk mobil yang memiliki nilai jual 0 hingga 100 juta rupiah. Jadi kisaran premi yang kita harus bayarkan maksimal hanya Rp3 juta saja.

d. Jasaraharja Putera
Asuransi Jasaraharja menjadi salah satu yang paling diminati karena menawarkan biaya premi yang harganya cukup murah dan termasuk lebih murah dibanding asuransi-asuransi mobil lainnya. Jasaraharja menawarkan dua jenis produk asuransi mobil, yakni TLO dan all risk yang bisa kita pilih sesuai kebutuhan.

e. Sinarmas
Asuransi Sinarmas mengenakan premi 2,5% dari harga mobil lebih dari 500 juta. Namun, jika harga mobil di bawah 500 juta rupiah, premi yang akan dikenakan adalah 3,25%. Menarik, bukan?

Pembayaran premi asuransi mobil bersifat tahunan, jadi dengan sekali bayar saja, maka mobil kita sudah terproteksi sepanjang tahun.

Kunjungi halaman asuransi mobil Lifepal untuk daftar pilihan asuransi mobil yang lebih lengkap, beserta perhitungan premi dan manfaat nya.'
        ],
        [
            'question' => 'Bagaimana cara membatalkan polis asuransi mobil saya?',
            'answer' => 'Boleh membatalkan polis asuransi secara sepihak. Tertanggung mesti mengajukan pembatalan polis asuransi itu kepada pihak asuransi secara tertulis.

Begitu pengajuan tertulis itu diterima, maka dalam jangka waktu lima hari ke depan pihak asuransi gak bertanggung jawab lagi terhadap risiko mobil tertanggung yang tercantum dalam polis.

Sangat disarankan kita sudah ada kepastian memilih asuransi mobil terbaik sebagai pengganti asurnasi yang lama sebelum mengajukan pembatalan polis asuransi. Kan jangan sampai mobil kenapa-apa di waktu polis asuransi sudah tak berlaku lagi dan di saat bersamaan belum punya polis asuransi yang baru.'
        ],
//         [
//             'question' => 'ผ่อนบอลลูนคืออะไร?',
//             'answer' => 'ปัจจุบันมีหลากหลายช่องทางให้ผู้ใช้รถและมอเตอร์ไซค์สามารถเลือกชำระภาษีได้ตามต้องการ ดังนี้
// 1.สำนักงานขนส่งทั่วประเทศ ไม่ว่ารถจะจดทะเบียนที่จังหวัดใดก็ตาม โดยผู้ขับขี่สามารถใช้บริการ เลื่อนล้อต่อภาษี (Drive Thru for Tax) โดยไม่ต้องลงจากรถ 
// 2.ที่ทำการไปรษณีย์
// 3.ธนาคารเพื่อการเกษตรและสหกรณ์การเกษตร 
// 4.ห้างสรรพสินค้าที่มีจุดบริการต่อภาษี  “ช้อปให้พอ แล้วต่อภาษี” (Shop Thru for Tax) อาทิ บิ๊กซี เซ็นทรัลเวิลด์ พาราไดส์พาร์ค และอื่น ๆ
// 5.ช่องทางออนไลน์ของกรมการขนส่งทางบก https://eservice.dlt.go.th/esvapp/login.jsf
// 6.ร้านค้าที่มีสัญลักษณ์เคาน์เตอร์เซอร์วิสทั่วประเทศ
// หากไม่นับการชำระภาษีที่สำนักงานขนส่ง ช่องทางอื่น ๆ จะมีค่าธรรมเนียมเพิ่มเติมและต้องรอสมุดทะเบียนรถกับป้ายสี่เหลี่ยมส่งมาถึงบ้านในภายหลัง แต่จะมีความสะดวกมากกว่า อาทิจุดบริการในห้างสรรพสินค้าส่วนใหญ่เปิดให้บริการวันเสาร์และอาทิตย์ ส่วนช่องทางออนไลน์เอื้อให้ผู้ใช้รถสามารถชำระภาษีที่ไหนก็ได้ เพียงแค่มีมือถือ แท็บเล็ต หรือแล็ปท็อปกับสัญญาณอินเตอร์เน็ต
// อย่างไรก็ตาม การต่อภาษีรถยนต์ออนไลน์หรือจุดบริการที่ไม่ใช่สำนักงานขนส่งจะให้บริการเฉพาะรถยนต์ที่มีอายุไม่เกิน 7 ปีและมอเตอร์ไซค์อายุไม่เกิน 5 ปีที่ไม่ต้องตรวจสภาพรถ '
//         ],
//         [
//             'question' => 'ต่อประกันภัยรายปีทำยังไง?',
//             'answer' => 'วิธีคำนวณภาษีรถยนต์ และอัตราการเสียภาษีรถยนต์นั้นขึ้นอยู่กับหลายปัจจัย เช่น ขนาดเครื่องยนต์คิดเป็นซีซี ประเภทรถ และอายุใช้งาน เป็นต้น 
// รถยนต์นั่งส่วนบุคคลไม่เกิน 7 คน มีอัตราดังนี้ 
// 1. จัดเก็บตามความจุกระบอกสูบ (ซีซี)

// 600 ซีซีแรก ซีซีละ 0.50 บาท
// 601 - 1,800 ซีซี ๆ ละ 1.50 บาท
// เกิน 1,800 ซีซี ๆ ละ 4.00 บาท
// ตัวอย่างการคำนวณภาษีรถยนต์ Mazda 3 เครื่องยนต์ 1,998 ซีซี รถใช้งานมานาน 3 ปี 
// ช่วง 600 ซีซีแรกอยู่ที่ 300 บาท
// ช่วง 601 – 1,800 ซีซีอยู่ที่ 1,798.50 บาท
// ช่วง 1,800 ซีซีขึ้นไปอยู่ที่ 792 บาท
// รวมค่าภาษีที่ต้องจ่ายทั้งหมดอยู่ที่ประมาณ 2,890.50 บาท

// ทั้งนี้ หากเป็นรถของนิติบุคคลที่มิได้เป็นผู้ให้เช่าซื้อจะจัดเก็บในอัตราสองเท่า นอกจากนี้ หากเป็นรถที่จดทะเบียนมาแล้ว 5 ปี ให้ได้รับการลดหย่อนภาษีประจำปีในปีต่อ ๆ ไป ดังนี้

// ปีที่ 6 ร้อยละ 10
// ปีที่ 7 ร้อยละ 20
// ปีที่ 8 ร้อยละ 30
// ปีที่ 9 ร้อยละ 40
// ปีที่ 10 และปีต่อ ๆ ไป ร้อยละ 50
// 2. จัดเก็บเป็นรายคันตามประเภทของรถ มีอัตราดังนี้ 

// รถจักรยานยนต์ส่วนบุคคล คันละ 100 บาท
// รถจักรยานยนต์สาธารณะ คันละ 100 บาท
// รถพ่วงของรถจักรยานยนต์ส่วนบุคคล คันละ 50 บาท 
// รถพ่วงนอกจากข้อ 2.3 คันละ 100 บาท
// รถบดถนน คันละ 200 บาท
// รถแทรกเตอร์ที่ใช้ในการเกษตร คันละ 50 บาท
// 3. จัดเก็บตามน้ำหนัก มีอัตราดังนี้

// น้ำหนักรถ (กิโลกรัม)	รถยนต์นั่งส่วนบุคคลเกิน 7 คน	รถยนต์รับจ้างระหว่างจังหวัด	รถยนต์รับจ้าง 	รถยนต์บรรทุกส่วนบุคคล รถลากจูง รถแทรกเตอร์ที่มิได้ใช้ในการเกษตร
// ไม่เกิน 500	150	450	185	300
// 501 – 750	 300	750 	310	450
// 750 – 1,000	450	1,050	450	600
// 1,001 – 1,250	800	1,350	560	750
// 1,251 – 1,500	1,000	1,650	685	900
// 1,501 – 1,750	1,300	2,100	875	1,050
// 1,751 – 2,000	1,600	2,550	1,060	1,350
// 2,001 – 2,500	1,900	3,000	1,250 	1,650
// 2,501 – 3,000	2,200	3,450	1,435	1,950
// 3,001 – 3,500	2,400 	3,900	1,625	2,250
// 3,501 – 4,000 	2,600	4,350 	1,810	2,550
// 4,001 – 4,500	2,800	4,800  	2,000	2,850
// 4,501 – 5,000 	3,000	5,250	2,185	3,150
// 5,001 – 6,000	3,200	5,700	2,375  	3,450
// 6,001 – 7,000  	3,400	6,150	2,560	3,750
// 7,001 ขึ้นไป	3,600	6,600	2,750	4,050
// 4. รถที่ขับเคลื่อนด้วยกำลังไฟฟ้า
// รถยนต์นั่งส่วนบุคคลไม่เกิน 7 คน ให้เก็บภาษีตามน้ำหนักของรถในอัตรารถยนต์นั่งส่วนบุคคลเกินเจ็ดคน'
//         ],
//         [
//             'question' => 'ต่อภาษีประจำปีอย่างไร?',
//             'answer' => 'โดยทั่วไปแล้ว ภาษีรถยนต์ไม่สามารถชำระแบบผ่อนรายเดือนได้ แต่สามารถเลือกชำระด้วยบัตรเครดิตซึ่งสามารถขอผ่อนชำระกับธนาคารหรือสถาบันการเงินเจ้าของบัตรได้ '
//         ],
//         [
//             'question' => 'ประกันชีวิตจำเป็นต้องทำหรือไม่?',
//             'answer' => 'ประเด็นนี้ถูกพูดถึงกันตั้งแต่อดีตจนถึงปัจจุบัน ล่าสุด กรมการขนส่งทางบกยืนยันว่า ถึงแม้ผู้ขับขี่จะมีใบสั่งค้างจ่ายจากกรณีใดก็ตาม อาทิ ใบสั่งจากกล้องตรวจจับความเร็ว  กรมฯ ได้อำนวยความสะดวกให้สามารถชำระภาษีรถประจำปีได้ แต่จะได้รับเอกสารหลักฐานแสดงการเสียภาษีประจำปีชั่วคราว ซึ่งมีอายุ 30 วัน นับแต่วันที่นายทะเบียนออกให้เท่านั้น ซึ่งหากชำระค่าปรับแล้วสามารถนำหลักฐานใบเสร็จการชำระค่าปรับมาแสดง เพื่อรับเครื่องหมายแสดงการเสียภาษีรถประจำปีฉบับจริงได้ในภายหลัง
// ผู้ขับขี่ที่สงสัยว่าตนเองมีใบสั่งค้างจ่ายหรือไม่ สามารถเข้าไปตรวจสอบได้ที่ https://ptm.police.go.th/eTicket/'
//         ],
      
    ];

    add_faq_json_ld($faqs);

    ob_start(); // Start output buffering
?>

    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css" integrity="sha384-k6RqeWeci5ZR/Lv4MR0sA0FfDOM7h6p3+NFldf3NOK5GT6Z3F68b9cJ2+Qp8V3b" crossorigin="anonymous" />

    <h2 class="wa-title-text">FAQ Asuransi</h2>

    <div class="faq-container">
        <?php foreach ($faqs as $faq): ?>
            <div class="faq-item">
                <div class="faq-question">
                    <?php echo esc_html($faq['question']); ?>
                </div>
                <div class="arrow-container">
                    <span class="arrow"><i class="fas fa-chevron-down"></i></span> <!-- Font Awesome down arrow -->
                </div>
                <div class="faq-answer"><?php echo esc_html($faq['answer']); ?></div>
                <hr /> <!-- Line separator between FAQs -->
            </div>
        <?php endforeach; ?>
    </div>

    <style>
        .faq-container {
            margin-top: 20px;
            border: 1px solid #ddd;
            border-radius: 5px;
            overflow: hidden;
        }

        .faq-item {
            cursor: pointer;
            position: relative;
        }

        .faq-item:last-child hr {
            display: none;
        }

        .faq-question {
            font-family: 'Roboto';
            padding: 19px 44px 19px 16px;
            font-size: 14px;
            color: #262626;
            line-height: 22px;
            font-weight: bold;
            position: relative;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .faq-answer {
            overflow: hidden;
            padding: 0 44px 16px 16px;
            font-size: 14px;
            color: #262626;
            line-height: 22px;
            display: none;
            /* Initially hidden */
            transition: all .2s;
        }

        .faq-item:hover {
            background-color: #f5f5f5;
        }

        .arrow-container {
            position: absolute;
            right: 15px;
            top: 50%;
            transform: translateY(-50%);
            transition: transform 0.2s ease;
        }

        .faq-item.active .arrow-container {
            transform: translateY(-50%) rotate(180deg);
            /* Rotate the arrow when active */
        }
    </style>

    <script>
        // Select all FAQ items
        document.addEventListener('DOMContentLoaded', () => {
            const faqItems = document.querySelectorAll('.faq-item'); // Adjust class for FAQ items

            faqItems.forEach(item => {
                item.addEventListener('click', () => {
                    const answer = item.querySelector('.faq-answer'); // Adjust class for FAQ answers

                    // Toggle visibility
                    if (answer.style.display === 'block') {
                        answer.style.display = 'none';
                        item.classList.remove('active');
                    } else {
                        // Hide other open answers
                        faqItems.forEach(i => {
                            i.querySelector('.faq-answer').style.display = 'none';
                            i.classList.remove('active');
                        });

                        // Show the current answer
                        answer.style.display = 'block';
                        item.classList.add('active');
                    }
                });
            });
        });
    </script>


<?php
    return ob_get_clean(); // Return the buffered output
}
add_shortcode('road_tax_faqs', 'road_tax_faqs');
?>

<?php
function road_tax_intro_shortcode()
{
    $private_cars = [
        ['engine_dis' => '1,000 cc and below', 'base_rate' => 'RM 20', 'road_tax' => '-', 'amount' => 'RM 20'],
        ['engine_dis' => '1,001 – 1,200', 'base_rate' => 'RM 55', 'road_tax' => '-', 'amount' => 'RM 55'],
        ['engine_dis' => '1,201 – 1,400', 'base_rate' => 'RM 70', 'road_tax' => '-', 'amount' => 'RM 70'],
        ['engine_dis' => '1,401 – 1,600', 'base_rate' => 'RM 90', 'road_tax' => '-', 'amount' => 'RM 90'],
        ['engine_dis' => '1,601 – 1,800', 'base_rate' => 'RM 200', 'road_tax' => 'RM 0.40', 'amount' => 'RM 200-280'],
        ['engine_dis' => '1,801 – 2,000', 'base_rate' => 'RM 280', 'road_tax' => 'RM 0.50', 'amount' => 'RM 280-380'],
        ['engine_dis' => '2,001 – 2,500', 'base_rate' => 'RM 380', 'road_tax' => 'RM 1.00', 'amount' => 'RM 381-880'],
        ['engine_dis' => '2,501 – 3,000', 'base_rate' => 'RM 880', 'road_tax' => 'RM 2.50', 'amount' => 'RM 882–2,130'],
        ['engine_dis' => '3,001 and above', 'base_rate' => 'RM 2,130', 'road_tax' => 'RM 4.00', 'amount' => 'RM 2,134 +'],
    ];
    $company_cars = [
        ['engine_dis' => '1,000 cc and below', 'base_rate' => 'RM 20', 'road_tax' => '-', 'amount' => 'RM 20'],
        ['engine_dis' => '1,001 – 1,200', 'base_rate' => 'RM 85', 'road_tax' => '-', 'amount' => 'RM 85'],
        ['engine_dis' => '1,201 – 1,400', 'base_rate' => 'RM 100', 'road_tax' => '-', 'amount' => 'RM 100'],
        ['engine_dis' => '1,401 – 1,600', 'base_rate' => 'RM 120', 'road_tax' => '-', 'amount' => 'RM 120'],
        ['engine_dis' => '1,601 – 1,800', 'base_rate' => 'RM 300', 'road_tax' => 'RM 0.30', 'amount' => 'RM 400 - 500'],
        ['engine_dis' => '1,801 – 2,000', 'base_rate' => 'RM 360', 'road_tax' => 'RM 0.40', 'amount' => 'RM 561 - 760'],
        ['engine_dis' => '2,001 – 2,500', 'base_rate' => 'RM 440', 'road_tax' => 'RM 0.80', 'amount' => 'RM 763 - 2260'],
        ['engine_dis' => '2,501 – 3,000', 'base_rate' => 'RM 840', 'road_tax' => 'RM 1.60', 'amount' => 'RM 2,267 – 6,010'],
        ['engine_dis' => '3,001 and above', 'base_rate' => 'RM 1,640', 'road_tax' => 'RM 1.60', 'amount' => 'RM 6,023 +'],
    ];
    $company_cars = [
        ['engine_dis' => '1,000 cc and below', 'base_rate' => 'RM 20', 'road_tax' => '-', 'amount' => 'RM 20'],
        ['engine_dis' => '1,001 – 1,200', 'base_rate' => 'RM 85', 'road_tax' => '-', 'amount' => 'RM 85'],
        ['engine_dis' => '1,201 – 1,400', 'base_rate' => 'RM 100', 'road_tax' => '-', 'amount' => 'RM 100'],
        ['engine_dis' => '1,401 – 1,600', 'base_rate' => 'RM 120', 'road_tax' => '-', 'amount' => 'RM 120'],
        ['engine_dis' => '1,601 – 1,800', 'base_rate' => 'RM 300', 'road_tax' => 'RM 0.30', 'amount' => 'RM 400 - 500'],
        ['engine_dis' => '1,801 – 2,000', 'base_rate' => 'RM 360', 'road_tax' => 'RM 0.40', 'amount' => 'RM 561 - 760'],
        ['engine_dis' => '2,001 – 2,500', 'base_rate' => 'RM 440', 'road_tax' => 'RM 0.80', 'amount' => 'RM 763 - 2260'],
        ['engine_dis' => '2,501 – 3,000', 'base_rate' => 'RM 840', 'road_tax' => 'RM 1.60', 'amount' => 'RM 2,267 – 6,010'],
        ['engine_dis' => '3,001 and above', 'base_rate' => 'RM 1,640', 'road_tax' => 'RM 1.60', 'amount' => 'RM 6,023 +'],
    ];
    $private_company_cars = [
        ['engine_dis' => '1,000 cc and below', 'base_rate' => 'RM 20', 'road_tax' => '-', 'amount' => 'RM 20'],
        ['engine_dis' => '1,001 – 1,200', 'base_rate' => 'RM 55', 'road_tax' => '-', 'amount' => 'RM 55'],
        ['engine_dis' => '1,201 – 1,400', 'base_rate' => 'RM 70', 'road_tax' => '-', 'amount' => 'RM 70'],
        ['engine_dis' => '1,401 – 1,600', 'base_rate' => 'RM 90', 'road_tax' => '-', 'amount' => 'RM 90'],
        ['engine_dis' => '1,601 – 1,800', 'base_rate' => 'RM 200', 'road_tax' => 'RM 0.40', 'amount' => 'RM 200 - 280'],
        ['engine_dis' => '1,801 – 2,000', 'base_rate' => 'RM 280', 'road_tax' => 'RM 0.50', 'amount' => 'RM 280 - 380'],
        ['engine_dis' => '2,001 – 2,500', 'base_rate' => 'RM 380', 'road_tax' => 'RM 1.00', 'amount' => 'RM 381 - 880'],
        ['engine_dis' => '2,501 – 3,000', 'base_rate' => 'RM 880', 'road_tax' => 'RM 2.50', 'amount' => 'RM 882 – 2,130'],
        ['engine_dis' => '3,001 and above', 'base_rate' => 'RM 2,130', 'road_tax' => 'RM 4.00', 'amount' => 'RM 2,134 +'],
    ];
    $private_east_malaysia_cars = [
        ['engine_dis' => '1,000 cc and below', 'base_rate' => 'RM 20', 'road_tax' => '-', 'amount' => 'RM 20'],
        ['engine_dis' => '1,001 – 1,200', 'base_rate' => 'RM 44', 'road_tax' => '-', 'amount' => 'RM 44'],
        ['engine_dis' => '1,201 – 1,400', 'base_rate' => 'RM 56', 'road_tax' => '-', 'amount' => 'RM 56'],
        ['engine_dis' => '1,401 – 1,600', 'base_rate' => 'RM 72', 'road_tax' => '-', 'amount' => 'RM 72'],
        ['engine_dis' => '1,601 – 1,800', 'base_rate' => 'RM 160', 'road_tax' => 'RM 0.32', 'amount' => 'RM 160 - 224'],
        ['engine_dis' => '1,801 – 2,000', 'base_rate' => 'RM 224', 'road_tax' => 'RM 0.25', 'amount' => 'RM 224 - 274'],
        ['engine_dis' => '2,001 – 2,500', 'base_rate' => 'RM 380', 'road_tax' => 'RM 0.50', 'amount' => 'RM 274 - 524'],
        ['engine_dis' => '2,501 – 3,000', 'base_rate' => 'RM 880', 'road_tax' => 'RM 1.00', 'amount' => 'RM 525 – 1,024'],
        ['engine_dis' => '3,001 and above', 'base_rate' => 'RM 2,130', 'road_tax' => 'RM 1.35', 'amount' => 'RM 1,025 +'],
    ];
    $privateandcompany_west_malaysia_cars = [
        ['engine_dis' => '1,000 cc and below', 'base_rate' => 'RM 20', 'road_tax' => '-', 'amount' => 'RM 20'],
        ['engine_dis' => '1,001 – 1,200', 'base_rate' => 'RM 42.50', 'road_tax' => '-', 'amount' => 'RM 42.50'],
        ['engine_dis' => '1,201 – 1,400', 'base_rate' => 'RM 50', 'road_tax' => '-', 'amount' => 'RM 50'],
        ['engine_dis' => '1,401 – 1,600', 'base_rate' => 'RM 60', 'road_tax' => '-', 'amount' => 'RM 60'],
        ['engine_dis' => '1,601 – 1,800', 'base_rate' => 'RM 165', 'road_tax' => 'RM 0.17', 'amount' => 'RM 165 - 199'],
        ['engine_dis' => '1,801 – 2,000', 'base_rate' => 'RM 199', 'road_tax' => 'RM 0.22', 'amount' => 'RM 199 - 243'],
        ['engine_dis' => '2,001 – 2,500', 'base_rate' => 'RM 243', 'road_tax' => 'RM 0.44', 'amount' => 'RM 243 - 463'],
        ['engine_dis' => '2,501 – 3,000', 'base_rate' => 'RM 463', 'road_tax' => 'RM 0.88', 'amount' => 'RM 464 – 903'],
        ['engine_dis' => '3,001 and above', 'base_rate' => 'RM 903', 'road_tax' => 'RM 1.20', 'amount' => 'RM 904 +'],
    ];
    ob_start();
?>
    <div class="intro-section">

        <h2 class="wa-title-text">Road Tax Introduction</h2>
        <div class="road-tax-content-box">
            <h4>Road Tax Introduction</h4>
            <p>The road tax is a tax paid by vehicle owners to use public roads. The tax amount varies depending on the vehicle type, vehicle
                <span id="road-tax-dots">...</span>
            </p>
            <div id="road-tax-more-text" class="road-tax-hidden-text">
                <p>specifications, vehicle registration location, type of ownership, and vehicle purpose. In Malaysia, the road tax is paid on an annual basis.

                    Vehicles with a valid road tax are required to have them displayed. For cars and trucks, the road tax sticker is displayed on the bottom right corner of the windscreen. The vehicle registration number, road tax price, and road tax expiry date are clearly written on the road tax sticker.

                    The road tax sticker is only meant for a one-time use. If removed or tampered with, the road tax sticker will tear apart, rendering it invalid.</p>
                <h4>Road Tax Calculation in Malaysia</h4>
                <p>In Malaysia, the road tax value is determined by several factors. Namely vehicle type, vehicle specifications, vehicle registration location, type of ownership, and vehicle purpose.</p>

                <p>Road users in West Malaysia and East Malaysia are taxed at a different rate. The road tax in East Malaysia is lower due to the conditions of the road and its supporting infrastructures.</p>
                <p>In both East and West Malaysia, the road tax increases as the engine displacement of the car increases. The road tax calculation is based on engine capacity.</p>
                <p>This form of road tax calculation has encouraged manufacturers to sell cars with smaller displacement engines that are usually turbocharged or assisted by a hybrid system. Most cars in Malaysia have less than 2.0-litre engine displacement.</p>

                <p>At minimum, the road tax for an engine-driven car is RM20 for cars with engine displacements of 1,000 cc and below. To calculate the road tax of a vehicle, the base rate and progressive rate will have to be combined.</p>

                <p>The progressive rate increases as the engine displacement increases. Cars that exceed 3.0 litres in West Malaysia are taxed RM4/cc for every cubic centimetre that exceeds the 3,000-cc mark.</p>

                <p>The type of vehicle ownership will also affect the road tax rate. A vehicle can be either private-owned or company-owned.</p>

                <p>The road tax for electric vehicles follow a different method of calculation that is based on the maximum power output of the vehicle.</p>
                <h4>Road tax payment schedule</h4>
                <p>The time of payment for the road tax depends on the date of registration of the vehicle. A car that is registered on 17 December will have its road tax expired by 16 December in the following year. The road tax is usually renewed together with the vehicle’s insurance.</p>
                <h4>How to renew Road tax?</h4>
                <p>The road tax renewal can be cone online via MyEG. The road tax will need to be renewed together with the car’s insurance.</p>
                <h4>What happens if road tax is not paid?</h4>
                <p>Without a road tax, the vehicle is not legally allowed to be driven on public roads. If a vehicle’s road tax has not been renewed for more than 36 months, a PUSPAKOM inspection will be required to renew it later on.</p>

                <div class="private-container">
                    <div class="private-row private-header">
                        <div class="private-cell text-center" colspan='4'>Private Cars in West Malaysia Road Tax</div>
                    </div>
                    <div class="private-row private-header">
                        <div class="private-cell text-center">Engine displacement (cc)</div>
                        <div class="private-cell text-center">Base Rate Progressive rates (per cc)</div>
                        <div class="private-cell text-center">Road Tax</div>
                        <div class="private-cell text-center">Amount</div>
                    </div>
                    <?php
                    // Loop through the array to display the data
                    foreach ($private_cars as $loan) {
                    ?>
                        <div class="private-row">
                            <div class="private-cell text-center"><?php echo $loan['engine_dis']; ?></div>
                            <div class="private-cell text-center"><?php echo $loan['base_rate']; ?></div>
                            <div class="private-cell text-center"><?php echo $loan['road_tax']; ?></div>
                            <div class="private-cell text-center"><?php echo $loan['amount']; ?></div>
                        </div>
                    <?php
                    }
                    ?>
                </div>
                <br>
                <!-- company cars data -->
                <div class="private-container">
                    <div class="private-row private-header">
                        <div class="private-cell text-center" colspan='4'>Company cars in West Malaysia road tax</div>
                    </div>
                    <div class="private-row private-header">
                        <div class="private-cell text-center">Engine displacement (cc)</div>
                        <div class="private-cell text-center">Base Rate Progressive rates (per cc)</div>
                        <div class="private-cell text-center">Road Tax</div>
                        <div class="private-cell text-center">Amount</div>
                    </div>
                    <?php
                    // Loop through the array to display the data
                    foreach ($company_cars as $loan) {
                    ?>
                        <div class="private-row">
                            <div class="private-cell text-center"><?php echo $loan['engine_dis']; ?></div>
                            <div class="private-cell text-center"><?php echo $loan['base_rate']; ?></div>
                            <div class="private-cell text-center"><?php echo $loan['road_tax']; ?></div>
                            <div class="private-cell text-center"><?php echo $loan['amount']; ?></div>
                        </div>
                    <?php
                    }
                    ?>
                </div>
                <br>
                <!-- private & company cars data -->
                <div class="private-container">
                    <div class="private-row private-header">
                        <div class="private-cell text-center" colspan='4'>Private and Company SUV/MPV/Pick-up in West Malaysia road tax</div>
                    </div>
                    <div class="private-row private-header">
                        <div class="private-cell text-center">Engine displacement (cc)</div>
                        <div class="private-cell text-center">Base Rate Progressive rates (per cc)</div>
                        <div class="private-cell text-center">Road Tax</div>
                        <div class="private-cell text-center">Amount</div>
                    </div>
                    <?php
                    // Loop through the array to display the data
                    foreach ($private_company_cars as $loan) {
                    ?>
                        <div class="private-row">
                            <div class="private-cell text-center"><?php echo $loan['engine_dis']; ?></div>
                            <div class="private-cell text-center"><?php echo $loan['base_rate']; ?></div>
                            <div class="private-cell text-center"><?php echo $loan['road_tax']; ?></div>
                            <div class="private-cell text-center"><?php echo $loan['amount']; ?></div>
                        </div>
                    <?php
                    }
                    ?>
                </div>
                <br>
                <!-- priavate east malaysia cars data -->
                <div class="private-container">
                    <div class="private-row private-header">
                        <div class="private-cell text-center" colspan='4'>Private cars in East Malaysia road tax</div>
                    </div>
                    <div class="private-row private-header">
                        <div class="private-cell text-center">Engine displacement (cc)</div>
                        <div class="private-cell text-center">Base Rate Progressive rates (per cc)</div>
                        <div class="private-cell text-center">Road Tax</div>
                        <div class="private-cell text-center">Amount</div>
                    </div>
                    <?php
                    // Loop through the array to display the data
                    foreach ($private_east_malaysia_cars as $loan) {
                    ?>
                        <div class="private-row">
                            <div class="private-cell text-center"><?php echo $loan['engine_dis']; ?></div>
                            <div class="private-cell text-center"><?php echo $loan['base_rate']; ?></div>
                            <div class="private-cell text-center"><?php echo $loan['road_tax']; ?></div>
                            <div class="private-cell text-center"><?php echo $loan['amount']; ?></div>
                        </div>
                    <?php
                    }
                    ?>
                </div>

                <br>
                <!-- priavate and company west malaysia cars data -->
                <div class="private-container">
                    <div class="private-row private-header">
                        <div class="private-cell text-center" colspan='4'>Private and Company SUV/MPV/Pick-up in West Malaysia road tax</div>
                    </div>
                    <div class="private-row private-header">
                        <div class="private-cell text-center">Engine displacement (cc)</div>
                        <div class="private-cell text-center">Base Rate Progressive rates (per cc)</div>
                        <div class="private-cell text-center">Road Tax</div>
                        <div class="private-cell text-center">Amount</div>
                    </div>
                    <?php
                    // Loop through the array to display the data
                    foreach ($privateandcompany_west_malaysia_cars as $loan) {
                    ?>
                        <div class="private-row">
                            <div class="private-cell text-center"><?php echo $loan['engine_dis']; ?></div>
                            <div class="private-cell text-center"><?php echo $loan['base_rate']; ?></div>
                            <div class="private-cell text-center"><?php echo $loan['road_tax']; ?></div>
                            <div class="private-cell text-center"><?php echo $loan['amount']; ?></div>
                        </div>
                    <?php
                    }
                    ?>
                </div>
            </div>
            <button id="road-tax-read-more-btn" class="road-tax-read-more-btn">Read More</button>
        </div>
    </div>
    <script>
        document.getElementById("road-tax-read-more-btn").addEventListener("click", function() {
            var moreText = document.getElementById("road-tax-more-text");
            var dots = document.getElementById("road-tax-dots");
            var btnText = document.getElementById("road-tax-read-more-btn");

            // Toggle visibility of the moreText
            if (moreText.classList.contains("road-tax-hidden-text")) {
                moreText.classList.remove("road-tax-hidden-text");
                dots.style.display = "none";
                btnText.innerHTML = "Read Less";
            } else {
                moreText.classList.add("road-tax-hidden-text");
                dots.style.display = "inline";
                btnText.innerHTML = "Read More";
            }
        });
    </script>
    <style>
        .road-tax-content-box h4 {
            font-family: 'Roboto' !important;
            font-size: 16px !important;
            color: #262626;
        }

        .private-container {
            border: 1px solid #ddd;
            /* max-width: 100%; */
            display: block;
            font-size: 10px;
        }

        .private-row {
            display: flex;
            justify-content: space-between;
            padding: 8px 0;
            border-bottom: 1px solid #ddd;
            background: #fff;
        }

        .private-row:last-child {
            border-bottom: none;
            /* Remove border for last row */
        }

        .private-cell {
            flex: 1;
            /* Equal width for all cells */
            padding: 5px;
            text-align: left;
        }

        .private-header .private-cell {
            font-weight: bold;
            background-color: #f9f9f9;
            /* Optional: Header background */
        }

        .private-header .private-cell:not(:last-child),
        .private-row .private-cell:not(:last-child) {
            border-right: 1px solid #ddd;
            margin-bottom: -8px;
            margin-top: -8px;
            /* Vertical line */
        }

        .road-tax-content-box {
            background-color: #F9F9F9;
            padding: 15px;
        }

        .road-tax-hidden-text {
            display: none;
        }

        .road-tax-read-more-btn {
            background: none;
            border: none;
            color: #007bff;
            cursor: pointer;
            font-size: 14px;
            padding: 0;
        }

        .road-tax-read-more-btn:hover {
            text-decoration: underline;
        }

        #road-tax-dots {
            display: inline;
        }
    </style>
<?php
    return ob_get_clean();
}
add_shortcode('road_tax_intro', 'road_tax_intro_shortcode');
