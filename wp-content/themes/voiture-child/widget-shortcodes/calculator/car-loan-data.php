<?php
include_once get_stylesheet_directory() . '/json-ld/faq-json-ld.php';

function car_loan_data($atts)
{
    $current_year = date('Y');
    $loan_data = [
        ['bank_name' => 'Bank Bukopin', 'interest_rate' => '6,78%'],
        ['bank_name' => 'OCBC NISP', 'interest_rate' => '4,9%'],
        ['bank_name' => 'Panin Bank', 'interest_rate' => '4,25%'],
        ['bank_name' => 'Mandiri', 'interest_rate' => '4,95%'],
//         ['bank_name' => 'Hong Leong Bank', 'interest_rate' => '3.24% p.a.'],
//         ['bank_name' => 'Maybank', 'interest_rate' => '3.4% p.a.'],
    ];
?>
    <h2 class="wa-title-text">Public Bank Car Loan Interest Rate <?php echo $current_year; ?></h2>
    <div class="loan-data-container">
        <div class="loan-data-row loan-data-header">
            <div class="loan-data-label">Bank Name</div>
            <div class="loan-data-value">Interest Rate</div>
        </div>
        <?php
        // Loop through the array to display the data
        foreach ($loan_data as $loan) {
        ?>
            <div class="loan-data-row">
                <div class="loan-data-label"><?php echo $loan['bank_name']; ?></div>
                <div class="loan-data-value"><?php echo $loan['interest_rate']; ?></div>
            </div>
        <?php
        }
        ?>
    </div>

    <style>
        .loan-data-container {
            max-height: 273px;
            overflow-y: scroll;
            /* Enable vertical scroll */
            overflow-x: hidden;
            /* Prevent horizontal scroll */
            border: 1px solid #ddd;
            /* max-width: 74%; */
            display: block;
        }

        .loan-data-container::-webkit-scrollbar {
            width: 0;
            height: 0;
        }

        .loan-data-container {
            scrollbar-width: none;
            /* Firefox */
        }

        .loan-data-row {
            display: flex;
            padding: 10px;
            border-bottom: 1px solid #ddd;
        }

        .loan-data-header {
            background-color: #f8f8f8;
            font-weight: bold;
        }

        .loan-data-label,
        .loan-data-value {
            flex: 1;
            display: flex;
            align-items: center;
        }

        .loan-data-label {
            text-align: left;
            border-right: 1px solid #ccc;
            padding-right: 15px;
            margin-top: -10px;
            margin-bottom: -10px;
        }

        .loan-data-value {
            text-align: right;
            padding-left: 15px;
        }

        .loan-data-row:first-child {
            border-top: 1px solid #ddd;
        }

        .loan-data-row:last-child {
            border-bottom: 1px solid #ddd;
        }
    </style>

<?php
}
add_shortcode('car_loan_data', 'car_loan_data');
?>

<?php
function car_payment_faqs()
{
    // FAQ data
   $faqs = [
    [
        'question' => 'Apa itu Kredit Mobil?',
        'answer' => 'Kredit mobil adalah cara membeli mobil dengan metode pembayaran dicicil atau diangsur. Metode kredit mobil ini bisa dilakukan untuk pembelian mobil baru maupun mobil bekas dari lembaga pembiayaan, seperti leasing dan lembaga perbankan.
Opsi kredit mobil bisa menjadi pilihan ketika kamu tidak memiliki dana yang cukup ketika harus membeli mobil. Produknya biasanya berupa kredit kendaraan bermotor (KKB).'
    ],
    [
        'question' => 'Bagaimana cara kerja kredit mobil di Indonesia?',
        'answer' => 'Biasanya ada dua pilihan, yaitu leasing dan lembaga perbankan.

Baik bank maupun leasing memang akan memiliki konsep dan peran yang sama untuk menyediakan dana talangan saat Anda ingin memiliki mobil tapi tidak mempunyai dana yang cukup untuk membayar kontan. Bank atau leasing ini akan membayarkan dahulu sebagian dari harga mobil yang diinginkan, kemudian secara berkala Anda akan membayar hutang dengan cicilan tiap bulan.
Selain cara kerja yang sama seperti yang disebutkan sebelumnya, persamaan kredit antara bank dan leasing adalah sama-sama mensyaratkan DP minimal 30% dari harga jual mobil yang sudah disepakati antara penjual dan pembeli.
Ketentuan ini sendiri telah diatur oleh Bank Indonesia yang tercantum dalam Surat Edaran Ekstern Nomor 14/10/DPNP yang menetapkan uang muka minimal 30% untuk pembelian kendaraan bermotor roda empat non produktif.'
    ],
    [
        'question' => 'Berapa jumlah kredit yang bisa saya dapatkan?',
        'answer' => 'Perhitungan angsuran pinjaman berdasar harga kendaraan bermotor. Untuk uang muka biasanya bank mensyaratkan minimal 30% dari harga mobil. Untuk bunga, biasanya bank menggunakan perhitungan bunga flat. Bunga flat kelihatannya kecil, namun bila dikonversi ke bunga efektif bisa jadi sangat besar.'
    ],
    [
        'question' => 'Apakah mengajukan kredit mobil dapat menpengaruhi nilai kredit Anda?',
        'answer' => 'Tidak, mengajukan kredit mobil tidak akan menpengaruhi nilai kredit Anda asal Anda membayar cicilan tiap bulan tepat waktu dan jumlah. Selain itu, kalau Anda bisa bayar cicilan dengan lancar, ini mungkin bisa membantu menambah nilai kreditmu. Pada masa depan kalau kamu mau meminjam uang dari bank akan menjadi semakin gampang. '
    ],
    [
        'question' => 'Bank mana yang memberikan kredit mobil terbaik?',
        'answer' => 'Berikut beberapa pilihan tempat untuk kredit mobil paling murah yang bisa membantu kita menemukan tempat kredit yang tepat.

a. Bank Bukopin KKB

b. Berdasarkan berbagai sumber yang dikumpulkan, Bank Bukopin merupakan penyedia kredit mobil yang paling murah. Dengan membayarkan DP minimum 30% dan suku bunga 4% per tahun, kita sudah bisa memiliki mobil baru maupun bekas. Jangka waktu yang berlaku hingga 5 tahun.

c. Panin Bank KPM
KPM Panin memberikan fasilitas kredit dengan sistem pinjaman. Persyaratan yang berlaku di bank ini cukup fleksibel serta mudah. Hanya dengan mengeluarkan down payment sebesar 30% dan suku bunga hanya 4,15% per tahun, mobil idaman kita sudah bisa dibawa pulang. Jangka waktu pinjamannya hingga 5 tahun.

d. Bank Jasa Jakarta KPM Pribadi
Agak berbeda dari sebelumnya, Bank Jasa Jakarta KPM Pribadi ini menerapkan suku bunga sebesar 4,20% per tahun dengan uang muka minimal sebesar 30%. Lebih istimewanya lagi, bank ini tidak menerapkan sistem penalti kalau kita ingin melakukan pelunasan di awal.

e. Bank Jasa Jakarta KPM In Advance
Bank Jasa Jakarta KPM In Advance akan membantu kita dalam mewujudkan impian untuk mendapatkan mobil baru dengan suku bunga sebesar 4,48% per tahun dan DP minimum 30%. Hal ini berlaku untuk kredit mobil pribadi maupun niaga.

f. BRI KKB Mobil
Dari pihak BRI KKB menawarkan pinjaman dana untuk kredit mobil baru ataupun bekas dengan persyaratan yang cukup mudah, cepat dan dilengkapi juga dengan fasilitas asuransi kendaraan. DP yang harus kita bayar hanya sebesar 25%, dengan beban suku bunga sebesar 4,99% per tahun dan tenor hingga 5 tahun.

g. CIMB Niaga KPM Smart Reguler
Kredit mobil idaman kita dapat diwujudkan dengan meminjam dana dari CIMB Niaga KPM. Dengan suku bunga yang ringan sebesar 5,55% per tahun dan down payment minimal 25%, serta persyaratan pengajuan yang cukup mudah akan semakin mendekatkan impian kita untuk mempunyai sebuah mobil baru. Di sini ada beragam masa tenor yang bisa kita pilih.

h. Bank OCBC-NISP KPM
Kita dapat menikmati kemudahan pengajuan pinjaman untuk kredit mobil hingga nominal 2 milyar rupiah. Dengan proses pengajuannya yang tidak sulit, kita bisa langsung mengendarai mobil idaman dengan DP sebesar 30% dan suku bunga sebesar 5,99% per tahun. Pihak Bank menyediakan beberapa varian tenor yang dapat disesuaikan dengan kebutuhan kita.

i. OTO Multiartha KPM
Perusahaan bidang otomotif yang satu ini, menyediakan fasilitas kredit dengan menawarkan suku bunga sebesar 8,25% per tahun dan juga uang muka sebesar 30%. Pelayanannya pun ramah, fleksibel dan mudah memanjakan kita sebagai konsumen.

j. BCA Finance KPM
BCA Finance KPM memberikan berbagai kemudahan buat kita untuk kredit mobil baru, hanya dengan minimum DP 30% dan suku bunganya sekitar 15% per tahun. Di sini tersedia juga kredit untuk mobil bekas dengan kualitas yang masih gres. Masa tenornya hingga 6 tahun. Yang lebih oke lagi, persyaratan pengajuannya mudah, cepat dan sistematis.'
    ],
    [
        'question' => 'Membeli mobil melalui mendaftar kredit mobil atau bayar sekaligus?',
        'answer' =>  "Keuntungan Membeli Mobil Secara Kredit:

a. Bila Untuk Tujuan Produktif, bisa Menambah Perputaran Modal. Dengan membeli kredit, tentu tak perlu membayar keseluruhan harga. Cukup dengan memberikan DP, sisanya bisa diangsur. 

b. Secara Psikologis ada Kesan Ringan. Ada kesan bahwa mencicil lebih ringan dibandingkan membayar langsung secara tunai.

c. Faktor Kecepatan Menjadi Salah Satu Alasan. Bagi sebagian orang, keinginan untuk segera memiliki sebuah mobil bisa jadi berubah jadi kebutuhan. Lewat leasing atau kredit cukup dengan membayar DP dan melengkapi persyaratan saja, sebuah mobil bisa langsung dikendarai.

d. Tak Perlu Membayar Total Harga Keseluruhan. Mengeluarkan uang sekali dengan besaran ratusan juta jelas akan mengurangi kemampuan ekonomi dalam satu waktu.

Kerugian Membeli Mobil Secara Kredit:

a. Menjadi Beban Keuangan Tambahan. Membeli apapun dengan cara kredit, termasuk mobil sekalipun tentu menghadirkan beban  tersendiri. Sebab meski mobil idaman sudah dimiliki, tapi ini belum sepenuhnya menjadi milik pribadi. 

b. Menambah Utang, secara Tidak Langsung. Dari segi ekonomi, keuangan yang sehat adalah bebas dari utang. Mencicil mobil sebenarnya sama saja dengan menambah beban utang yang muncul dari setiap angsuran pembayaran mobil tersebut.

c. Bila Bunga Tinggi dan Kondisi Sosio Politik Tidak Stabil maka Beban akan Semakin Besar. Di mana kondisi perkreditan sangat terkait dengan kestabilan perbankan dalam negeri. Pada saat yang sama bila muncul hal yang mengguncang kondisi sosio politik dalam negeri tentu bisa saja kredit mobil yang sedang berjalan turut terdampak. Setidaknya imbas yang paling mungkin adalah bila terjadi kenaikan suku bunga kredit.

Bayar sekaligus

Beli Mobil Tunai, Bebas Dari Beban Utang, Bahkan Ada DiskonJika mampu membeli mobil secara tunai, sebaiknya Anda tidak perlu mengambil kredit kendaraan. Jangan tergoda untuk membeli banyak produk namun secara kredit. Lebih baik Anda fokus membeli secara tunai produk yang memang Anda butuhkan. Beberapa keuntungan membeli mobil secara tunai berikut ini bisa menjadi motivasi tersendiri bagi Anda untuk tidak tergoda membeli mobil secara kredit.

a. Bebas Beban, Mudah Untuk Diuangkan Kembali.

b. Hindari Shock Therapy, dengan Tidak ada Suku Bunga Menanjak.

c. Beberapa Dealer Punya Program Diskon untuk Pembelian secara Tunai."
    ],
//     [
//         'question' => 'Balloon Payment คืออะไร?',
//         'answer' => 'การผ่อนแบบ Balloon คือ การลดภาระเงินต้นครึ่งหนึ่ง เช่น กู้เงิน 500,000 บาท แบงก์จะให้ชำระเพียง 250,000 บาทก่อน และงวดสุดท้ายต้องจ่ายส่วนที่เหลือ  

// เหมาะกับรถที่มีราคาขายต่อสูง หรือบริษัทที่ต้องการเปลี่ยนรถใหม่ทุกครั้งที่ครบสัญญา'
//     ],
//     [
//         'question' => 'ต่อประกันรถยนต์รายปีอย่างไร?',
//         'answer' => 'บริษัทไฟแนนซ์จะเก็บเงินค่าประกันภัยปีละครั้ง หากต้องการเปลี่ยนบริษัทประกัน ต้องแจ้งไฟแนนซ์และดำเนินการด้วยตนเอง'
//     ],
//     [
//         'question' => 'ต่อภาษีรถยนต์รายปีอย่างไร?',
//         'answer' => 'เล่มทะเบียนรถจะอยู่กับไฟแนนซ์จนกว่าผ่อนครบ ดังนั้นบริษัทไฟแนนซ์จะเป็นผู้ดำเนินการต่อภาษีรถให้ และเรียกเก็บค่าภาษีจากเราทุกปี'
//     ],
//     [
//         'question' => 'ประกันชีวิตจำเป็นหรือไม่?',
//         'answer' => 'การทำประกันชีวิตระหว่างผ่อนรถเป็นทางเลือกเสริม ไม่บังคับ หากเลือกทำจะช่วยคุ้มครองหากเกิดอุบัติเหตุหรือเสียชีวิต โดยประกันจะช่วยชำระค่างวดที่เหลือ'
//     ],
//     [
//         'question' => 'ขาดผ่อนรถได้กี่เดือน?',
//         'answer' => 'หากขาดผ่อน 3 เดือนติดต่อกัน จะมีช่วงเวลา 30 วันในการติดตามหนี้ หากยังไม่ชำระ รถจะถูกยึดและเครดิตเสียทันที'
//     ],
//     [
//         'question' => 'สามารถคืนรถระหว่างผ่อนชำระได้หรือไม่?',
//         'answer' => 'หากไม่สามารถผ่อนต่อได้ ควรเจรจากับบริษัทไฟแนนซ์เพื่อปรับโครงสร้างหนี้ หากคืนรถ ไฟแนนซ์จะนำไปขาย หากราคาขายต่ำกว่ายอดหนี้ที่เหลือ เราต้องชดเชยส่วนต่าง'
//     ],
//     [
//         'question' => 'รีไฟแนนซ์คืออะไร?',
//         'answer' => 'รีไฟแนนซ์คือการขอลดค่างวดโดยขยายระยะเวลาผ่อน เช่น เหลือ 30 งวด ผ่อนเดือนละ 10,000 บาท หากรีไฟแนนซ์เป็น 48 งวด จะเหลือเดือนละ 6,250 บาท  

// ทั้งนี้ ควรศึกษารายละเอียดของแต่ละธนาคารก่อนตัดสินใจ'
//     ]
];


    add_faq_json_ld($faqs);

    ob_start(); // Start output buffering
?>

    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css" integrity="sha384-k6RqeWeci5ZR/Lv4MR0sA0FfDOM7h6p3+NFldf3NOK5GT6Z3F68b9cJ2+Qp8V3b" crossorigin="anonymous" />

    <h2 class="wa-title-text">FAQ Kredit Mobil dan Cicilan Mobil</h2>

    <div class="car-payment-faq-container">
        <?php foreach ($faqs as $faq): ?>
            <div class="car-payment-faq-item">
                <div class="car-payment-faq-question">
                    <?php echo esc_html($faq['question']); ?>
                    <span class="arrow-container">
                        <i class="fas fa-chevron-down"></i>
                    </span>
                </div>
                <div class="car-payment-faq-answer"><?php echo esc_html($faq['answer']); ?></div>
            </div>
        <?php endforeach; ?>
    </div>

    <style>
        /* FAQ Container */
        .car-payment-faq-container {
            margin-top: 20px;
            border: 1px solid #ddd;
            border-radius: 5px;
            overflow: hidden;
        }

        /* FAQ Items */
        .car-payment-faq-item {
            padding: 15px 20px;
            cursor: pointer;
            border-bottom: 1px solid #ddd;
            position: relative;
            transition: background-color 0.3s ease;
        }

        .car-payment-faq-item:last-child {
            border-bottom: none;
        }

        /* Hover Effect */
        .car-payment-faq-item:hover {
            background-color: #f9f9f9;
        }

        /* FAQ Question */
        .car-payment-faq-question {
            font-weight: bold;
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-size: 14px;
            color: #262626;
            line-height: 22px;
            font-weight: bold;
        }

        /* Arrow Styling */
        .arrow-container {
            transition: transform 0.3s ease;
        }

        .car-payment-faq-item.active .arrow-container {
            transform: rotate(180deg);
        }

        /* FAQ Answer */
        .car-payment-faq-answer {
            display: none;
            margin-top: 10px;
            color: #555;
            line-height: 1.6;
            transition: all 0.3s ease;
            font-family: "Roboto";
            font-size: 14px;
        }
    </style>

    <script>
        document.addEventListener('DOMContentLoaded', () => {
            const faqItems = document.querySelectorAll('.car-payment-faq-item');

            faqItems.forEach(item => {
                item.addEventListener('click', () => {
                    const answer = item.querySelector('.car-payment-faq-answer');

                    // Toggle visibility
                    if (answer.style.display === 'block') {
                        answer.style.display = 'none';
                        item.classList.remove('active');
                    } else {
                        // Hide other open answers
                        faqItems.forEach(i => {
                            i.querySelector('.car-payment-faq-answer').style.display = 'none';
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
add_shortcode('car_payment_faqs', 'car_payment_faqs');
?>
<?php
function car_loan_intro_shortcode()
{
    ob_start();
?>
<div class="intro-section">
        <h2 class="wa-title-text">Buying a Car and Applying for a Loan</h2>
        <div class="content-box">
            <h4>Buying a car and applying for a loan: an easy matter that is easy to understand</h4>
            <p>When buying cars and motorcycles in Thailand, financial service providers play a crucial role. Since most customers must pay for their cars in installments,
                <span id="dots">...</span>
            </p>
            <div id="more-text" class="hidden-text">
                <p>the leasing company acts as a middleman, paying the car manufacturer before allowing customers to pay in installments along with interest. While wealthy individuals may purchase cars with cash, car financing is essential for most people.
                </p>
                <h4>What is Car Financing?</h4>
                <p>
                    Car financing is similar to borrowing money through various financial sources. Whether the finance application is approved depends on the company's policies. In a hire purchase agreement, the buyer is considered a lessee who must pay installments and interest until full ownership is achieved.
                </p>
                <p>
                    Financing is divided into two main types:
                    <ul>
                        <li>Car financing directly from the car company.</li>
                        <li>Financing from financial service providers.</li>
                    </ul>
                    Many service providers also offer financing for used cars, either directly from used car dealers or between individual buyers and sellers who need financial assistance.
                </p>
                <h4>Steps to Apply for Car Financing</h4>
                <p>
                    Car dealerships, both new and used, typically assist buyers in preparing the necessary documents for a loan application, making the process smoother. 
                </p>
                <p>
                    Required documents usually include ID cards, employment certificates, and other financial documents depending on the loan type. Once submitted, the service provider will assess them thoroughly.
                </p>
                <p>
                    The approval process typically takes no more than seven working days. If there are no issues, the loan will be approved. However, if rejected, applicants may attempt to secure financing from other providers.
                </p>
            </div>
            <div class="read-more-btn-loan">
                <button id="read-more-btn" class="read-more-btn">Read More</button>		
            </div>
        </div>
    </div>
    <script>
        document.getElementById("read-more-btn").addEventListener("click", function() {
            var moreText = document.getElementById("more-text");
            var dots = document.getElementById("dots");
            var btnText = document.getElementById("read-more-btn");

            // Toggle visibility of the moreText
            if (moreText.classList.contains("hidden-text")) {
                moreText.classList.remove("hidden-text");
                dots.style.display = "none";
                btnText.innerHTML = "Read Less";
            } else {
                moreText.classList.add("hidden-text");
                dots.style.display = "inline";
                btnText.innerHTML = "Read More";
            }
        });
    </script>
    <style>
        .intro-section {
            /* max-width: 74%; */
        }

        .content-box {
            background-color: #F9F9F9;
            padding: 15px;

        }
		.content-box p {
		   margin-top: -15px;
		}

        .content-box h4 {
            font-family: 'Roboto' !important;
            font-size: 16px !important;
            color: #262626;
        }

        .hidden-text {
            display: none;
        }

        .read-more-btn {
            background: none;
            border: none;
            color: #576b95;
            cursor: pointer;
            font-size: 14px;
		    margin-top: -15px;
			font-weight:700;
        }
		.read-more-btn-loan{
			display:flex;
			justify-content:end;
		}
        .read-more-btn:hover {
            text-decoration: underline;
        }

        #dots {
            display: inline;
        }
    </style>
<?php
    return ob_get_clean();
}
add_shortcode('car_loan_intro', 'car_loan_intro_shortcode');
