<?php
include_once get_stylesheet_directory() . '/json-ld/faq-json-ld.php';

function enqueue_car_valuation_faq_css()
{
    wp_enqueue_style('car-valuation-faq-style', get_stylesheet_directory_uri() . '/widget-shortcodes/cars-for-sale/car-valuation/car-valuation-faq/car-valuation-faq.css', array(), '1.0', 'all');
}

function car_valuation_faqs_shortcode($atts)
{
    enqueue_car_valuation_faq_css();

    $car_valuation_faqs = get_car_valuation_faqs();

    add_faq_json_ld($car_valuation_faqs);

    ob_start();
?>
    <h2 class="wa-title-text">Car Valuation FAQs</h2>
    <div id="listing-detail-description" class="description inner">
        <div class="description-inner">
            <div class="description-inner-wrapper">
                <div class="accordion">
                    <div class="acc">
                        <?php
                        foreach ($car_valuation_faqs as $faq) {
                            $question = $faq['question'];
                            $answer = $faq['answer'];

                            echo '<div class="accordion-item">';
                            echo '<input type="checkbox" id="faq-question-' . $faq['id'] . '">';
                            echo '<label for="faq-question-' . $faq['id'] . '" class="accordion-header">' . $question . '</label>';

                            echo '<div class="accordion-content">' . $answer . '</div>';
                            echo '</div>';
                        }
                        wp_reset_postdata();
                        ?>
                    </div>
                </div>
            </div>
        </div>
    </div>
<?php

    $output = ob_get_clean(); // Get the buffered content

    return $output; // Return the content for the shortcode
}
add_shortcode('car_valuation_faqs', 'car_valuation_faqs_shortcode');

function get_car_valuation_faqs()
{
    $car_valuation_faqs = [
        [
            'id' => 1,
            'question' => "How to check car market value in Malaysia with WapCar's car market value calculator?",
            'answer' => "Just select the make, model, variant, engine size, and year of manufacture into the guide. The estimated market value for your car will be automatically calculated."
        ],
        [
            'id' => 2,
            'question' => "Can I look up car value market price by VIN number?",
            'answer' => "The VIN number tells you the location of manufacture, year of manufacture, model code and the car’s serial number. Some of that information can be used to determine a car’s market value. However, you don’t have to find the VIN number of a car to determine the market value of a car.
            <br><br>The information needed to determine the market value of a car is usually already available on the Vehicle Ownership Certificate (VOC). What is more important is to make sure the engine number and chassis number match with the information on the VOC."
        ],
        [
            'id' => 3,
            'question' => "How is car value estimated?",
            'answer' => "The value of a car can be estimated with the following information – year of manufacture, make, and model. The make and model will determine a base price and the year of manufacture will determine the depreciation amount of the car."
        ],
        [
            'id' => 4,
            'question' => "Why does mileage affect car value?",
            'answer' => "Mileage indicates how much the car has been used, and subsequently indicates the amount of wear and tear that the car has experienced. The bearings, bushings, belts, and other mechanical parts on high-mileage cars are usually at the end of their life cycle and will need replacing soon.
            <br><br>High mileage cars will also have wear and tear on the exterior from paint chips which make them less desirable."
        ],
        [
            'id' => 5,
            'question' => "Does value of a car affect insurance?",
            'answer' => "Yes, it does. Cars with higher market value will need more coverage and hence a higher premium. For some cars of high value, only the comprehensive insurance package is made available."
        ],
        [
            'id' => 6,
            'question' => "Can I trade in my car if it is not paid off?",
            'answer' => "The simple answer is no. You will have to settle the balance of the car loan with the bank. Usually, the value of the car is used to pay off the remaining balance.
            <br><br>There are two ways this could turn out – positive equity or negative equity.
            <br><br>Positive equity is when the car’s value exceeds the loan balance and negative equity is when the car’s value is less than the loan balance. In a negative equity situation, you will have to fork out extra money to pay off the loan. Either way, the loan will need to be paid off. What matters to the bank is that the loan is paid off."
        ],
        [
            'id' => 7,
            'question' => "Will a car’s market value affect my chance of loan approval?",
            'answer' => "Yes, it would. And it is important to know that different banks will value the same car differently – some higher than others. The reason it affects your loan approval is that the car serves as a collateral in case you default on your loan.
            <br><br>If the bank doesn’t value your car as much you do, they will often charge a higher interest, lower the loan amount, or increase the monthly installment."
        ],
        [
            'id' => 8,
            'question' => "What brand of car has the highest resale value?",
            'answer' => "Obvious brands that hold their value well are Toyota and Honda. Cars that have a good reputation of reliability and high desirability will naturally have better resale value. Inversely, cars with poor reputation of reliability are unable to fetch high resale values."
        ],
        [
            'id' => 9,
            'question' => "Is it better to sell used car to a private buyer or to a dealership?",
            'answer' => "If you want a quick sale, sell to a dealer. They are often ready to make an offer. If you’re hopeful for more cash, you can try selling to a private buyer. Do keep in mind, that the process of finding a serious buyer can take some time."
        ],
        [
            'id' => 10,
            'question' => "What are things that will reduce the market value of a used car?",
            'answer' => "The obvious factors are high mileage, old age of car, and history of accidents or breakdowns. High mileage and old age increase the wear and tear. Accidents could be only cosmetic only, but serious accidents could compromise the structural integrity of the car, and hence your safety."
        ],
        [
            'id' => 11,
            'question' => "Will modifying my car reduce the market value?",
            'answer' => "Yes, it would. For a car’s market value to be preserved, it needs to be in its original condition. Buyers often don’t have the same taste as you. Modifying the car also tampers with the warranty (if there’s still any) and hence reduces the desirability. There are exceptional cases for this rule, but generally modifications reduce the value of the car."
        ],
        [
            'id' => 12,
            'question' => "What is the market value of a 2020 used car in Malaysia?",
            'answer' => "The year of the car itself is insufficient to determine its market value. You will need to know the make, model and variant of the car to get the estimated market value. Once you have this information, use the WapCar Market Value Guide to get a figure."
        ],
        [
            'id' => 13,
            'question' => "How much is the market value of the car with an accident?",
            'answer' => "You will need to the know the basic details of the car first (make, model, year variant, etc.) What we can tell you is that an accident will reduce the resale value of the car.
            <br><br>If you see signs of an accident on a used car you would like to purchase, inspect thoroughly to make sure the safety of the car has not been compromised."
        ],
        [
            'id' => 14,
            'question' => "How can I increase the resale value of my car?",
            'answer' => "Unfortunately, there is not much you can do to increase the market value of your car. Not even new tyres or new suspension will increase its value. What you can do to avoid the market value going down is to take good care of the car and drive it carefully. Even so, you are minimising the car’s depreciation by a small amount."
        ],
        [
            'id' => 15,
            'question' => "Why do some cars have poor resale value?",
            'answer' => "Some cars are just less desirable than others. Even if the car holds significant value for you, the market disagrees. Some cars are just not made to have high resale value. A reputation of poor reliability and expensive maintenance will often drive the market value of a used car down."
        ],
    ];

    return $car_valuation_faqs;
}
