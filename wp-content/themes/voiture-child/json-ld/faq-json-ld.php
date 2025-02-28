<?php

function add_faq_json_ld($faqs)
{
    $json_ld = [
        "@context" => "https://schema.org",
        "@type" => "FAQPage",
        "mainEntity" => []
    ];

    foreach ($faqs as $faq) {
        $json_ld['mainEntity'][] = [
            "@type" => "Question",
            "name" => $faq['question'],
            "acceptedAnswer" => [
                "@type" => "Answer",
                "text" => $faq['answer']
            ]
        ];
    }

    // Convert the PHP array to JSON format
    $json_ld_script = json_encode($json_ld, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);

    echo "<script type='application/ld+json'>$json_ld_script</script>";
}
