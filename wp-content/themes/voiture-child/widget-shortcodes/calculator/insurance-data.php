<?php
include_once get_stylesheet_directory() . '/json-ld/faq-json-ld.php';

function insurance_faqs()
{
    // FAQ data
    $faqs = [
        [
            'question' => 'What is the purpose of car insurance?',
            'answer' => 'Car insurance serves to offer financial protection to the parties involved in a traffic incident. The priority is the well-being of the other person involved in the accident. Depending on the type of coverage, some parties are not protected (usually yourself).'
        ],
        [
            'question' => 'Is a car insurance compulsory in Malaysia?',
            'answer' => 'Yes. Car insurance is compulsory in Malaysia. Without a car insurance policy, you are unable to obtain a road tax. No insurance means no road tax. Without a road tax, it is illegal to drive your car on public roads.'
        ],
        [
            'question' => 'What are the three types of car insurance?',
            'answer' => 'The three types of car insurance in Malaysia are:
Third-party insurance
Third-party, fire, and theft insurance
Comprehensive insurance'
        ],
        [
            'question' => 'What is covered in a third-party car insurance?',
            'answer' => 'A third-party car insurance only covers the financial losses of the other person involved in accident with you. Your car’s damages and your medical bills are not covered.'
        ],
        [
            'question' => 'What does a comprehensive insurance cover?',
            'answer' => 'It covers the financial cost of damage to your own car on top of the financial damage to the other party in the event of a traffic accident. Fire and theft insurance are included in a comprehensive insurance. Windscreen insurance is usually not included in a comprehensive insurance.'
        ],
        [
            'question' => 'Do I need to insure my car if I’m not using it?',
            'answer' => 'If it is only parked in a private property, an insurance is not required. You will need a car insurance if the car is used on public roads. A minimum of a third-party insurance is required to make the car road legal. '
        ],
        [
            'question' => 'How are insurance premiums calculated in Malaysia?',
            'answer' => 'Insurance premiums are calculated based on the sum insured on the car and what type of coverage is being subscribed to. Risk factors affecting the insurance premium of the car include the car price (current value), car type (sports car/family car), car registration location, age of driver, and car engine displacement.'
        ],
        [
            'question' => 'How do I get cheap car insurance?',
            'answer' => 'The less the sum insured, the cheaper the insurance premium will be. Cheaper cars generally cost less to insure. The insurance premium can also be lowered if you are assessed to be at lower risk of an accident.
Aa a matter of fact, discounts are given to drivers who prove to be at lower risk in the form of NCD'
        ],
        [
            'question' => 'What is an NCD?',
            'answer' => 'The NCD is a no-claim discount that is given to drivers that have never claimed their insurance/be involved in an incident. After 1 year of no claims, a 25% discount is given on the premium on the following year. The discounted amount increases up to 55% after 5 years of no insurance claims.'
        ],
        [
            'question' => 'How do I renew my car insurance in Malaysia?',
            'answer' => 'Your car insurance can be renewed online on the websites of the respective insurance providers. You will just need to fill in the details of yourself and your car to get a quotation. Different car insurance providers will offer different rates. Once you’ve decided which car insurance you’d like to purchase, you can do so at the respective website.'
        ],
        [
            'question' => 'How do I claim my insurance in the event of an accident?',
            'answer' => 'In the event of an accident, check if you and the other party have suffered from any injuries first. You will need to contact your insurance provider, lodge a police report, document the accident, gather relevant documents, and submit the relevant documents to your insurer.'
        ],
        [
            'question' => 'Do you have to pay insurance on a used car in Malaysia?',
            'answer' => 'All cars that are used on public roads are required to have an insurance policy. The insurance policy is non-transferrable since a new owner will be assessed at a different risk level.'
        ],
        [
            'question' => 'Do used cars cost more to insure in Malaysia?',
            'answer' => 'It depends. It could be less and it could be more depending on the insurance provider.'
        ],
        [
            'question' => 'What happens if I get into an accident without car insurance?',
            'answer' => 'This usually happens when you drive a car that you do not own (borrowed). You are not an insured driver. In this case, you will have to bear the financial damage yourself. Car rentals are usually insured (as long as you are the insured driver) so you don’t have to worry about that 

Do you really need full coverage for auto insurance?'
        ],
        [
            'question' => 'Does my car insurance cover me in the event of a flood?',
            'answer' => 'Unfortunately, the standard third-party insurance and comprehensive insurance do not cover the damages to your car in the event of a flood. You will need to subscribe to special perils insurance for natural disasters like so.'
        ],
    ];

    add_faq_json_ld($faqs);

    ob_start(); // Start output buffering
?>

    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css" integrity="sha384-k6RqeWeci5ZR/Lv4MR0sA0FfDOM7h6p3+NFldf3NOK5GT6Z3F68b9cJ2+Qp8V3b" crossorigin="anonymous" />

    <h2 class="wa-title-text">Insurance FAQ</h2>

    <div class="insurance-faq-container">
        <?php foreach ($faqs as $faq): ?>
            <div class="insurance-faq-item">
                <div class="insurance-faq-question">
                    <?php echo esc_html($faq['question']); ?>
                    <!-- <span class="arrow"><i class="fas fa-chevron-down"></i></span> -->
                </div>
                <div class="insurance-arrow-container">
                    <span class="arrow"><i class="fas fa-chevron-down"></i></span> <!-- Font Awesome down arrow -->
                </div>
                <div class="insurance-faq-answer"><?php echo esc_html($faq['answer']); ?></div>
                <hr /> <!-- Line separator between FAQs -->
            </div>
        <?php endforeach; ?>
    </div>

    <style>
        .insurance-faq-container {
            /* max-width: 74%; */
            margin-top: 20px;
            border: 1px solid #ddd;
            /* Outer border for the entire container */
            border-radius: 5px;
            overflow: hidden;
            /* Ensure border radius works */
        }

        .insurance-faq-item {

            cursor: pointer;
            position: relative;
            /* Make it a positioned element */
        }

        .insurance-faq-item:last-child hr {
            display: none;
            /* Hide the last line separator */
        }

        .insurance-faq-question {
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

        .insurance-faq-answer {
            overflow: hidden;
            padding: 0 44px 16px 16px;
            font-size: 14px;
            color: #262626;
            line-height: 22px;
            transition: all .2s;
        }

        .insurance-faq-item:hover {
            background-color: #f5f5f5;
            /* Optional hover effect */
        }

        .insurance-arrow-container {
            position: absolute;
            /* Position the arrow absolutely */
            right: 15px;
            /* Align to the right with a margin */
            top: 50%;
            /* Center vertically */
            transform: translateY(-50%);
            /* Adjust to center */
            transition: transform 0.2s ease;
            /* Transition for rotation */
        }

        .insurance-faq-item.active .insurance-arrow-container {
            transform: translateY(-50%) rotate(180deg);
            /* Rotate arrow when active */
        }
    </style>

    <script>
        // Ensure all answers are initially hidden
        document.querySelectorAll('.insurance-faq-answer').forEach(answer => {
            answer.style.display = 'none';
        });

        document.querySelectorAll('.insurance-faq-item').forEach(item => {
            item.addEventListener('click', () => {
                const answer = item.querySelector('.insurance-faq-answer');
                const isActive = item.classList.contains('active');

                // Close all other FAQs
                document.querySelectorAll('.insurance-faq-item.active').forEach(activeItem => {
                    if (activeItem !== item) {
                        activeItem.classList.remove('active');
                        activeItem.querySelector('.insurance-faq-answer').style.display = 'none';
                    }
                });

                // Toggle current FAQ
                if (isActive) {
                    // If the current FAQ is active, close it
                    answer.style.display = 'none';
                    item.classList.remove('active');
                } else {
                    // Otherwise, open it
                    answer.style.display = 'block';
                    item.classList.add('active');
                }
            });
        });
    </script>


<?php
    return ob_get_clean(); // Return the buffered output
}
add_shortcode('insurance_faqs', 'insurance_faqs');
?>

<?php
function insurance_intro_shortcode()
{
    $no_claim_discount = [
        ['coverage_duration' => '1st year', 'discount' => '25%'],
        ['coverage_duration' => '2nd year', 'discount' => '30%'],
        ['coverage_duration' => '3rd year', 'discount' => '38.33%'],
        ['coverage_duration' => '4th year', 'discount' => '45%'],
        ['coverage_duration' => '5th year', 'discount' => '55%'],
    ];
    ob_start();
?>
    <div class="intro-section">

        <h2 class="wa-title-text">Insurance Introduction</h2>
        <div class="insurance-content-box">
            <h4>Car Insurance Introduction</h4>
            <p>Car insurance provides financial protection in the event of a traffic accident that causes physical damage or injury. Car insurance is
                <span id="insurance-dots">...</span>
            </p>
            <div id="insurance-more-text" class="insurance-hidden-text">
                <p>compulsory in Malaysia with the minimum requirement being the third-party insurance. Without a car insurance policy, the road tax could not be renewed for your car.</p>
                <h4>Types of Car Insurance in Malaysia</h4>
                <p>There are three main types of car insurance in Malaysia:</p>
                <p>Third party insurance</p></br>
                <p>Third party, fire, and theft</p><br>
                <p>Comprehensive</p><br>
                <h4>Third Party Insurance</h4>
                <p>In the event of injury or physical damage caused by a traffic collision where you are at fault, your insurance is used to compensate the financial losses caused to other party involved. Hence the name third party insurance. A police report will have to be made prior to making an insurance claim. </p>
                <br>
                <p>The third-party insurance is the most basic form of car insurance that only compensates the damages of the other party involved. Your personal damages are not covered.</p>
                <h4>Third party, fire, and theft insurance</h4>
                <p>“Third party, fire, and theft insurance” is a step up from the basic third-party insurance with fire and theft insurance included. As the name suggests, damage to your own car in a collision is still not covered. </p>
                <h4>Comprehensive insurance</h4>
                <p>Comprehensive insurance is the most complete form of car insurance. In addition to the coverage provided by “third party insurance” and “third party, fire, and theft insurance”, damage to your own vehicle is also covered by the insurance policy.</p>
                <h4>Factors affecting car insurance premium</h4>
                <p>The insurance premium for your car will be affected by several factors like:</p>
                <br>
                <p>Car gross market value</p>
                <p>Type of car insurance</p>
                <p>Car engine displacement</p>
                <p>Your location</p>
                <p>The car’s depreciation will reduce its gross market value, a higher insurance coverage will incur a higher premium, and a bigger engine displacement incur a higher premium.</p>
                <p>Your location will determine the risk of an incident, thus affecting the insurance premium. All of these factors are assessed when you use our insurance calculator.</p>
                <h4>No-claim discount (NCD)</h4>
                <p>A no-claim discount is a discount that you will receive if the car insurance has never been claimed. The car is assessed to be at a lower risk of an incident. The NCD starts at 25% after one year and increases up to 55 % after 5 years of no claims.</p>

                <div class="dis-container">
                    <div class="dis-row dis-header">
                        <div class="dis-label">Coverage Duration</div>
                        <div class="dis-value">Discount</div>
                    </div>
                    <?php
                    // Loop through the array to display the data
                    foreach ($no_claim_discount as $loan) {
                    ?>
                        <div class="dis-row">
                            <div class="dis-label"><?php echo $loan['coverage_duration']; ?></div>
                            <div class="dis-value"><?php echo $loan['discount']; ?></div>
                        </div>
                    <?php
                    }
                    ?>
                </div>
                <h4>Insurance add-ons</h4>
                <p>On top of the three insurance types mentioned earlier, add-on car insurances can also be subscribed to such as:</p>
                <br>
                <p>Windscreen insurance</p>
                <p>Driver insurance</p>
                <p>Audio system insurance</p>
                <p>(Special Perils) (Natural disaster) insurance</p>
                <p>Strike, riot, and civil commotion insurance</p>
                <p>Legal liability for Passenger Act of Negligence</p>
                <p>Different drivers will have different insurance needs. With a good insurance package, you can have peace of mind in case of any unexpected incidents.</p>
            </div>
            <button id="insurance-read-more-btn" class="insurance-read-more-btn">Read More</button>
        </div>
    </div>
    <script>
        document.getElementById("insurance-read-more-btn").addEventListener("click", function() {
            var moreText = document.getElementById("insurance-more-text");
            var dots = document.getElementById("insurance-dots");
            var btnText = document.getElementById("insurance-read-more-btn");

            // Toggle visibility of the moreText
            if (moreText.classList.contains("insurance-hidden-text")) {
                moreText.classList.remove("insurance-hidden-text");
                dots.style.display = "none";
                btnText.innerHTML = "Read Less";
            } else {
                moreText.classList.add("insurance-hidden-text");
                dots.style.display = "inline";
                btnText.innerHTML = "Read More";
            }
        });
    </script>
    <style>
        .dis-container {
            max-height: 273px;
            border: 1px solid #ddd;
            /* max-width: 100%; */
            display: block;
        }

        .dis-row {
            display: flex;
            padding: 10px;
            border-bottom: 1px solid #ddd;
            background: #fff;
        }

        .dis-header {
            background-color: #f8f8f8;
            font-weight: bold;
        }

        .dis-label,
        .dis-value {
            flex: 1;
            display: flex;
            align-items: center;
        }

        .dis-label {
            text-align: left;
            border-right: 1px solid #ccc;
            padding-right: 15px;
            margin-top: -10px;
            margin-bottom: -10px;
        }

        .dis-value {
            text-align: right;
            padding-left: 15px;
        }

        .dis-row:first-child {
            border-top: 1px solid #ddd;
        }

        .dis-row:last-child {
            border-bottom: 1px solid #ddd;
        }



        .insurance-content-box {
            background-color: #F9F9F9;
            padding: 15px;
            font-family: 'Roboto';
            color: #262626;
        }

        .insurance-content-box h4 {
            background-color: #F9F9F9;
            font-family: 'Roboto';
            color: #262626;
        }

        .insurance-hidden-text {
            display: none;
        }

        .insurance-read-more-btn {
            background: none;
            border: none;
            color: #007bff;
            cursor: pointer;
            font-size: 14px;
            padding: 0;
        }

        .insurance-read-more-btn:hover {
            text-decoration: underline;
        }

        #insurance-dots {
            display: inline;
        }
    </style>
<?php
    return ob_get_clean();
}
add_shortcode('insurance_intro', 'insurance_intro_shortcode');
