<?php
include_once get_stylesheet_directory() . '/json-ld/faq-json-ld.php';

function enqueue_ev_faq_assets()
{
	 wp_enqueue_style('ev-faq-style', get_stylesheet_directory_uri() . '/widget-shortcodes/ev/css/ev-faq.css');
    wp_enqueue_script('ev-faq-script', get_stylesheet_directory_uri() . '/widget-shortcodes/ev/js/ev-faq.js', array('jquery'), null, true);
}
function ev_faq_shortcode($atts)
{
    enqueue_ev_faq_assets();

    $args = array(
        'post_type' => 'faq',
        'posts_per_page' => 10,
        'meta_query' => array(
            array(
                'key'     => 'faqs_type',
                'value'   => 'EV',
                'compare' => '=',
            ),
        ),
        'meta_key'   => 'weight',
        'orderby'    => 'meta_value_num',
        'order'      => 'ASC',
    );
    $faq_query = new WP_Query($args);

    // Prepare FAQ data for JSON-LD
    $ev_faqs = array();
    if ($faq_query->have_posts()) {
        while ($faq_query->have_posts()) {
            $faq_query->the_post();
			$post_id = get_the_ID();
			$meta_data = get_post_meta($post_id);
			$question = isset($meta_data['question'][0]) ? esc_html($meta_data['question'][0]) : 'No question available';
			$answer = isset($meta_data['answer'][0]) ? wp_kses_post($meta_data['answer'][0]) : 'No answer available';

            $ev_faqs[] = array(
                'id' => $post_id,
                'question' => $question,
                'answer' => $answer,
            );
        }
    }

    wp_reset_postdata();

    // Add FAQ JSON-LD only if there are FAQs
    if (!empty($ev_faqs)) {
        add_faq_json_ld($ev_faqs);
    } else {
        return ''; // Return empty string if no FAQs are found
    }

    ob_start();
    ?>
    <div class="popular-ev-head">
        <h2 class="wa-title-text">EV Car FAQs</h2>
    </div>

    <div id="listing-detail-description" class="description inner">
        <div class="description-inner">
            <div class="description-inner-wrapper">
                <div class="accordion">
                    <div class="acc">
                        <?php
                        if (!empty($ev_faqs)) {
                            foreach ($ev_faqs as $ev_faq) {
                                $post_id = $ev_faq['id'];
                                $question = $ev_faq['question'];
                                $answer = $ev_faq['answer'];
		
                                echo '<div class="accordion-item">';
                                echo '<input type="checkbox" id="faq-question-' . esc_attr($post_id) . '">';

                                echo '<div class="accordion-header">
								<div>
								<i class="fa-solid fa-bolt" style="color: rgb(42, 205, 172); margin-right: 14px;"></i>
								</div>
                                    <div class="question">
                                        ' . esc_html($question) . '
                                    </div>
                                </div>';

                                echo '<div class="accordion-content">' . $answer . '</div>';
                                echo '</div>';
                            }
                        } else {
                            echo '<p>No FAQs found for this category.</p>';
                        }
                        ?>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        document.addEventListener("DOMContentLoaded", function () {
            const accordionItems = document.querySelectorAll(".accordion-item");

            accordionItems.forEach(item => {
                const header = item.querySelector(".accordion-header");
                const content = item.querySelector(".accordion-content");
                const checkbox = item.querySelector("input[type='checkbox']");

                header.addEventListener("click", function () {
                    const isOpen = checkbox.checked;
                    accordionItems.forEach(innerItem => {
                        const innerCheckbox = innerItem.querySelector("input[type='checkbox']");
                        const innerContent = innerItem.querySelector(".accordion-content");

                        innerCheckbox.checked = false;
                        innerContent.style.maxHeight = null;
                    });

                    if (!isOpen) {
                        checkbox.checked = true;
                        content.style.maxHeight = content.scrollHeight + "px";
                    } else {
                        content.style.maxHeight = null;
                    }
                });
            });
        });
    </script>

    <?php
    return ob_get_clean();
}
add_shortcode('ev_car_faqs', 'ev_faq_shortcode');
